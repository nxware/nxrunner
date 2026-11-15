"""
  DbBrowser: Oberflaeche fuer DB-API-2.0-Datenbanken (PEP 249)

    DbBrowser(sqlite3.connect('x.db'))
    DbBrowser('sqlite:///pfad/x.db')            # auch 'x.db' oder ':memory:'
    DbBrowser('postgresql://user:pw@host:5432/db')   # psycopg2 bzw. psycopg
    DbBrowser('mysql://user:pw@host:3306/db')        # pymysql

  Seiten: SQL ausfuehren, DB-Objekte (Tabellen, Views, ...), Tabelle mit Struktur und Daten als Grid,
  Datensaetze anlegen, bearbeiten und loeschen.

  Ops (siehe @op): sql, objects, structure, rows, insert_row, update_row, delete_row

  Datensaetze werden ueber den Primaerschluessel adressiert, bei SQLite ohne Primaerschluessel ueber rowid,
  sonst ueber alle Spalten. Trifft ein update/delete mehr als einen Datensatz, wird zurueckgerollt.
  In Formularen steht der Wert \\N fuer NULL.
"""
import json
import sys
import urllib.parse

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient.runner import op

NULL_TOKEN = '\\N'
ROWID = '__rowid__'


def connect(connection_str: str):
    """ Oeffnet eine DB-API-Verbindung anhand eines Connection-Strings """
    url = urllib.parse.urlparse(connection_str)
    scheme = url.scheme.split('+')[0].lower()
    if scheme in ('postgres', 'postgresql'):
        try:
            import psycopg2
            return psycopg2.connect(connection_str.replace(url.scheme + '://', 'postgresql://', 1))
        except ImportError:
            import psycopg
            return psycopg.connect(connection_str.replace(url.scheme + '://', 'postgresql://', 1))
    if scheme in ('mysql', 'mariadb'):
        import pymysql
        return pymysql.connect(host=url.hostname or 'localhost', port=url.port or 3306,
                               user=urllib.parse.unquote(url.username or ''),
                               password=urllib.parse.unquote(url.password or ''),
                               database=url.path.lstrip('/') or None)
    import sqlite3
    if scheme == 'sqlite':
        path = connection_str[len('sqlite://'):]
        path = path[1:] if path.startswith('/') and len(path) > 1 else path
        return sqlite3.connect(path or ':memory:', check_same_thread=False)
    return sqlite3.connect(connection_str, check_same_thread=False)


class Dialect:
    """ Unterschiede der Datenbanken: Parameter-Stil, Quoting und Metadaten """

    def __init__(self, conn):
        self.conn = conn
        module = type(conn).__module__.split('.')[0]
        self.module = module
        self.paramstyle = getattr(sys.modules.get(module), 'paramstyle', 'qmark')
        if module == 'sqlite3':
            self.name = 'sqlite'
        elif module in ('pymysql', 'MySQLdb', 'mysql'):
            self.name = 'mysql'
        elif module in ('psycopg2', 'psycopg', 'pg8000'):
            self.name = 'postgres'
        else:
            self.name = 'generic'

    def quote(self, ident: str) -> str:
        if self.name == 'mysql':
            return '`' + str(ident).replace('`', '``') + '`'
        return '"' + str(ident).replace('"', '""') + '"'

    def param(self, i: int) -> str:
        """ Platzhalter fuer den i-ten Parameter (Werte werden immer als Liste uebergeben) """
        if self.paramstyle == 'qmark':
            return '?'
        if self.paramstyle == 'numeric':
            return ':' + str(i + 1)
        if self.paramstyle in ('format', 'pyformat'):
            return '%s'
        return '?'

    def objects_sql(self) -> str:
        if self.name == 'sqlite':
            return ("SELECT name, type, tbl_name FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name")
        if self.name == 'mysql':
            return ("SELECT table_name, table_type, table_schema FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() ORDER BY table_type, table_name")
        return ("SELECT table_name, table_type, table_schema FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY table_type, table_name")

    def structure_sql(self, table: str):
        """ :return: (sql, params) """
        if self.name == 'sqlite':
            return f"PRAGMA table_info({self.quote(table)})", []
        schema = "DATABASE()" if self.name == 'mysql' else "current_schema()"
        return ("SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, "
                "(SELECT COUNT(*) FROM information_schema.key_column_usage k "
                " JOIN information_schema.table_constraints t ON t.constraint_name = k.constraint_name "
                "  AND t.table_schema = k.table_schema AND t.table_name = k.table_name "
                " WHERE t.constraint_type = 'PRIMARY KEY' AND k.table_schema = c.table_schema "
                "  AND k.table_name = c.table_name AND k.column_name = c.column_name) AS pk "
                "FROM information_schema.columns c "
                f"WHERE c.table_name = {self.param(0)} AND c.table_schema = {schema} "
                "ORDER BY c.ordinal_position"), [table]

    def map_structure_row(self, row) -> dict:
        if self.name == 'sqlite':
            return dict(name=row[1], type=row[2], notnull=bool(row[3]), default=row[4], pk=bool(row[5]))
        return dict(name=row[0], type=row[1], notnull=str(row[2]).upper() == 'NO', default=row[3],
                    pk=bool(row[4]))

    def normalize_type(self, value: str) -> str:
        v = str(value).lower()
        return {'base table': 'table', 'view': 'view'}.get(v, v)


class DbBrowser(r.BaseJobExecutor):
    """
      Zeigt DB-Objekte und Tabellen (Struktur und Daten), fuehrt SQL aus, bearbeitet Datensaetze.
      :param connection: DB-API-2.0-Connection oder Connection-String (siehe connect)
    """

    def __init__(self, connection=None, type='dbbrowser', args: u.Args = None, page_size: int = 100):
        super().__init__(type)
        if connection is None:
            connection = (args or u.Args()).get('db_url', ':memory:')
        self.connection_str = connection if isinstance(connection, str) else None
        self.conn = connect(connection) if isinstance(connection, str) else connection
        self.dialect = Dialect(self.conn)
        self.page_size = page_size
        self.define_row_sigs()

    # --- Datenbank -----------------------------------------------------------------------------------

    def query(self, sql: str, params=None, limit: int = None):
        """ :return: (columns, rows, rowcount), columns ist leer bei Anweisungen ohne Ergebnis """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params or [])
            if cur.description is None:
                rowcount = cur.rowcount
                self.conn.commit()
                return [], [], rowcount
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(limit) if limit else cur.fetchall()
            return cols, [list(row) for row in rows], cur.rowcount
        except Exception:
            self.rollback()
            raise
        finally:
            cur.close()

    def rollback(self):
        try:
            self.conn.rollback()
        except Exception:
            pass

    def list_objects(self) -> list:
        _, rows, _ = self.query(self.dialect.objects_sql())
        return [dict(name=row[0], type=self.dialect.normalize_type(row[1]), parent=row[2]) for row in rows]

    def table_names(self) -> list:
        return [o['name'] for o in self.list_objects() if o['type'] in ('table', 'view')]

    def check_table(self, table: str) -> str:
        """ Nur bekannte Tabellen/Views zulassen, da Bezeichner nicht als Parameter uebergeben werden koennen """
        if table not in self.table_names():
            raise ValueError(f"Unknown table: {table}")
        return table

    def get_structure(self, table: str) -> list:
        sql, params = self.dialect.structure_sql(self.check_table(table))
        _, rows, _ = self.query(sql, params)
        return [self.dialect.map_structure_row(row) for row in rows]

    def key_columns(self, table: str, structure: list = None) -> list:
        """ Primaerschluessel, bei SQLite sonst rowid, sonst alle Spalten """
        structure = structure if structure is not None else self.get_structure(table)
        pk = [c['name'] for c in structure if c['pk']]
        if pk:
            return pk
        if self.dialect.name == 'sqlite' and self.object_type(table) == 'table':
            return [ROWID]
        return [c['name'] for c in structure]

    def object_type(self, table: str) -> str:
        for o in self.list_objects():
            if o['name'] == table:
                return o['type']
        return ''

    def select_rows(self, table: str, limit: int = 100, offset: int = 0):
        """ :return: (columns, rows), bei SQLite ohne Primaerschluessel mit Spalte __rowid__ """
        structure = self.get_structure(table)
        q = self.dialect.quote
        select = ', '.join(q(c['name']) for c in structure) or '*'
        if self.key_columns(table, structure) == [ROWID]:
            select = f'rowid AS {ROWID}, ' + select
        sql = f"SELECT {select} FROM {q(table)} LIMIT {int(limit)} OFFSET {int(offset)}"
        cols, rows, _ = self.query(sql)
        return cols, rows

    def count_rows(self, table: str) -> int:
        _, rows, _ = self.query(f"SELECT COUNT(*) FROM {self.dialect.quote(self.check_table(table))}")
        return int(rows[0][0])

    # --- Aendern -------------------------------------------------------------------------------------

    def check_columns(self, table: str, names, allow_rowid=False) -> list:
        known = [c['name'] for c in self.get_structure(table)] + ([ROWID] if allow_rowid else [])
        unknown = [n for n in names if n not in known]
        if unknown:
            raise ValueError(f"Unknown columns: {', '.join(unknown)}")
        return list(names)

    def where_clause(self, key: dict, start: int = 0):
        """ :return: (sql, params), NULL-Werte werden mit IS NULL verglichen """
        parts, params = [], []
        for name, value in key.items():
            ident = 'rowid' if name == ROWID else self.dialect.quote(name)
            if value is None:
                parts.append(f"{ident} IS NULL")
            else:
                parts.append(f"{ident} = {self.dialect.param(start + len(params))}")
                params.append(value)
        return ' AND '.join(parts), params

    def modify_one(self, sql: str, params: list) -> int:
        """ Fuehrt update/delete aus, rollt zurueck wenn mehr als ein Datensatz betroffen ist """
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            if cur.rowcount > 1:
                self.rollback()
                raise ValueError(f"{cur.rowcount} rows would be affected, rolled back")
            self.conn.commit()
            return cur.rowcount
        except Exception:
            self.rollback()
            raise
        finally:
            cur.close()

    def update_record(self, table: str, key: dict, values: dict) -> int:
        self.check_columns(table, list(values.keys()))
        self.check_columns(table, list(key.keys()), allow_rowid=True)
        if not values:
            raise ValueError("No values")
        q, p = self.dialect.quote, self.dialect.param
        sets = ', '.join(f"{q(k)} = {p(i)}" for i, k in enumerate(values.keys()))
        where, where_params = self.where_clause(key, len(values))
        return self.modify_one(f"UPDATE {q(table)} SET {sets} WHERE {where}", list(values.values()) + where_params)

    def delete_record(self, table: str, key: dict) -> int:
        self.check_columns(table, list(key.keys()), allow_rowid=True)
        if not key:
            raise ValueError("No key")
        where, params = self.where_clause(key)
        return self.modify_one(f"DELETE FROM {self.dialect.quote(table)} WHERE {where}", params)

    def insert_record(self, table: str, values: dict) -> int:
        self.check_columns(table, list(values.keys()))
        q, p = self.dialect.quote, self.dialect.param
        if values:
            cols = ', '.join(q(k) for k in values.keys())
            ph = ', '.join(p(i) for i in range(len(values)))
            sql = f"INSERT INTO {q(table)} ({cols}) VALUES ({ph})"
        else:
            sql = f"INSERT INTO {q(table)} DEFAULT VALUES"
        _, _, rowcount = self.query(sql, list(values.values()))
        return rowcount

    @staticmethod
    def form_values(data: dict, prefix: str = 'v_') -> dict:
        """ v_<spalte>=wert aus dem Job, \\N wird zu NULL """
        values = u.dict_extract_by_prefix(data, prefix)
        return {k: (None if v == NULL_TOKEN else v) for k, v in values.items()}

    # --- Ops -----------------------------------------------------------------------------------------

    @op("Fuehrt eine SQL-Anweisung aus (max. limit Zeilen)",
        result={'success': 'bool', 'message': 'str', 'columns': 'list', 'rows': 'list', 'rowcount': 'int'})
    def sql(self, sql: str = '', limit: int = 1000):
        try:
            cols, rows, rowcount = self.query(sql, limit=limit)
            return self.success(columns=cols, rows=self.json_rows(rows), rowcount=rowcount)
        except Exception as e:
            return self.fail(str(e))

    @op("Listet die DB-Objekte (Tabellen, Views, Indizes, ...)", result={'success': 'bool', 'objects': 'list'})
    def objects(self):
        return self.success(objects=self.list_objects())

    @op("Struktur (Spalten) einer Tabelle", result={'success': 'bool', 'message': 'str', 'columns': 'list'})
    def structure(self, table: str = ''):
        try:
            return self.success(columns=self.get_structure(table))
        except Exception as e:
            return self.fail(str(e))

    @op("Datensaetze einer Tabelle", result={'success': 'bool', 'message': 'str', 'columns': 'list', 'rows': 'list'})
    def rows(self, table: str = '', limit: int = 100, offset: int = 0):
        try:
            cols, rows = self.select_rows(table, limit, offset)
            return self.success(columns=cols, rows=self.json_rows(rows))
        except Exception as e:
            return self.fail(str(e))

    def execute_update_row(self, data: dict):
        """ table, key (JSON {spalte: wert}), v_<spalte>=neuer Wert """
        try:
            n = self.update_record(data['table'], u.load_to_dict(data.get('key')), self.form_values(data))
            return self.success(f"{n} row(s) updated", rowcount=n)
        except Exception as e:
            return self.fail(str(e))

    def execute_delete_row(self, data: dict):
        """ table, key (JSON {spalte: wert}) """
        try:
            n = self.delete_record(data['table'], u.load_to_dict(data.get('key')))
            return self.success(f"{n} row(s) deleted", rowcount=n, ui_reload=True)
        except Exception as e:
            return self.fail(str(e))

    def execute_insert_row(self, data: dict):
        """ table, v_<spalte>=Wert (leere Felder werden weggelassen) """
        try:
            values = {k: v for k, v in self.form_values(data).items() if v != ''}
            n = self.insert_record(data['table'], values)
            return self.success(f"{n} row(s) inserted", rowcount=n)
        except Exception as e:
            return self.fail(str(e))

    def define_row_sigs(self):
        from nwebclient import dev as d
        res = dict(success='bool', message='str', rowcount='int')
        self.define_sig(d.PStr('op', 'update_row'), d.PStr('table'), d.PStr('key')).set_description(
            "Datensatz aendern: key als JSON, neue Werte als v_<spalte>").r(**res)
        self.define_sig(d.PStr('op', 'delete_row'), d.PStr('table'), d.PStr('key')).set_description(
            "Datensatz loeschen: key als JSON {spalte: wert}").r(**res)
        self.define_sig(d.PStr('op', 'insert_row'), d.PStr('table')).set_description(
            "Datensatz anlegen: Werte als v_<spalte>").r(**res)

    # --- Darstellung ---------------------------------------------------------------------------------

    @staticmethod
    def cell(value):
        """ Wert fuer JSON bzw. Grid """
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return f"<{len(bytes(value))} bytes>"
        return str(value)

    def json_rows(self, rows) -> list:
        return [[self.cell(v) for v in row] for row in rows]

    def grid_rows(self, cols: list, rows: list) -> list:
        return [{c: self.cell(v) for c, v in zip(cols, row)} for row in rows]

    def form_value(self, value) -> str:
        return NULL_TOKEN if value is None else str(self.cell(value))

    def part_index(self, p: b.Page, params={}):
        with p.section(h="SQL"):
            self.ui_sql_form(p, params.get('sql', ''))
            if params.get('sql'):
                self.ui_sql_result(p, params['sql'])
        with p.section(h="Objekte"):
            self.ui_objects(p)

    def ui_sql_form(self, p: b.Page, sql: str):
        p(f'<form method="get" action="">')
        p(w.hidden('type', self.type))
        p(w.hidden(self.type, 'index'))
        p(w.textarea(w.escape(sql), name='sql', rows='6', style='width: 100%; font-family: monospace;',
                     title="SQL-Anweisung, z.B. SELECT * FROM tabelle"))
        p(w.tag('input', '', type='submit', value="Execute", title="SQL ausfuehren und Ergebnis als Grid anzeigen"))
        p('</form>')

    def ui_sql_result(self, p: b.Page, sql: str):
        try:
            cols, rows, rowcount = self.query(sql, limit=1000)
        except Exception as e:
            p.div("Error: " + w.escape(str(e)), _class='error')
            return
        if not cols:
            p.prop("Rows affected", rowcount)
            return
        p.prop("Rows", len(rows))
        p.grid(self.grid_rows(cols, rows), [w.Grid.col(c, sortable=False) for c in cols])

    def ui_objects(self, p: b.Page):
        try:
            objects = self.list_objects()
        except Exception as e:
            p.div("Error: " + w.escape(str(e)), _class='error')
            return

        def name_link(o):
            if o['type'] in ('table', 'view'):
                return w.a(w.escape(o['name']), self.link(self.part_table, table=o['name']),
                           hint=f"{o['type']} anzeigen: Struktur und Daten")
            return w.escape(o['name'])
        p.grid(objects, [w.Grid.col('name', title="Name", value=name_link, html=True),
                         w.Grid.col('type', title="Typ"), w.Grid.col('parent', title="Tabelle/Schema")],
               empty="Keine Objekte")

    def part_table(self, p: b.Page, params={}):
        table = params.get('table', '')
        try:
            structure = self.get_structure(table)
        except Exception as e:
            p.div("Error: " + w.escape(str(e)), _class='error')
            return
        p.h2(f"Tabelle: {w.escape(table)}")
        p(w.a("Zurueck", self.link(self.part_index), hint="Zur Objektliste"))
        with p.section(h="Struktur"):
            p.grid(structure, ['name', 'type', 'notnull', 'default', 'pk'])
        with p.section(h="Daten"):
            self.ui_table_data(p, table, structure, params)
        p.pre('', id='result')

    def ui_table_data(self, p: b.Page, table: str, structure: list, params: dict):
        limit = int(params.get('limit', self.page_size))
        offset = int(params.get('offset', 0))
        cols, rows = self.select_rows(table, limit, offset)
        key_cols = self.key_columns(table, structure)
        editable = self.object_type(table) == 'table'
        if editable:
            p(w.a("Neuer Datensatz", self.link(self.part_edit, table=table), hint="Datensatz anlegen"))
        self.ui_pager(p, table, offset, limit, self.count_rows(table))
        grid_rows = self.grid_rows(cols, rows)
        grid_cols = [w.Grid.col(c) for c in cols if c != ROWID]
        if editable:
            grid_cols.append(w.Grid.col(None, title='', html=True, sortable=False,
                                        value=lambda row: self.ui_row_actions(table, key_cols, row)))
        p.grid(grid_rows, grid_cols, params=params, empty="Keine Datensaetze")

    def ui_pager(self, p: b.Page, table: str, offset: int, limit: int, total: int):
        links = [f"{offset + 1 if total else 0} - {min(offset + limit, total)} / {total}"]
        if offset > 0:
            links.append(w.a("Zurueck", self.link(self.part_table, table=table, limit=limit,
                                                  offset=max(0, offset - limit)), hint="Vorherige Seite"))
        if offset + limit < total:
            links.append(w.a("Weiter", self.link(self.part_table, table=table, limit=limit,
                                                 offset=offset + limit), hint="Naechste Seite"))
        p.div(' | '.join(links))

    def row_key(self, key_cols: list, row: dict) -> dict:
        return {k: row.get(k) for k in key_cols}

    def ui_row_actions(self, table: str, key_cols: list, row: dict) -> str:
        key = json.dumps(self.row_key(key_cols, row))
        edit = w.a("Edit", self.link(self.part_edit, table=table, key=key), hint="Datensatz bearbeiten")
        job = json.dumps(dict(type=self.type, op='delete_row', table=table, key=key))
        delete = w.button_js("Delete", f'if (confirm("Datensatz loeschen?")) exec_job({job});',
                             hint="Datensatz loeschen (mit Rueckfrage)")
        return edit + ' ' + delete

    def load_record(self, table: str, key: dict, structure: list) -> dict:
        self.check_columns(table, list(key.keys()), allow_rowid=True)
        q = self.dialect.quote
        where, params = self.where_clause(key)
        select = ', '.join(q(c['name']) for c in structure)
        cols, rows, _ = self.query(f"SELECT {select} FROM {q(table)} WHERE {where}", params, limit=2)
        if len(rows) != 1:
            raise ValueError(f"{len(rows)} rows found for key {json.dumps(key)}")
        return dict(zip(cols, rows[0]))

    def part_edit(self, p: b.Page, params={}):
        """ Ohne key: neuen Datensatz anlegen """
        table = params.get('table', '')
        key = u.load_to_dict(params.get('key')) if params.get('key') else None
        try:
            structure = self.get_structure(table)
            record = self.load_record(table, key, structure) if key else {}
        except Exception as e:
            p.div("Error: " + w.escape(str(e)), _class='error')
            return
        p.h2(("Bearbeiten: " if key else "Neu: ") + w.escape(table))
        p(w.a("Zurueck", self.link(self.part_table, table=table), hint="Zur Tabelle"))
        job = dict(type=self.type, op='update_row' if key else 'insert_row', table=table)
        if key:
            job['key'] = json.dumps(key)
        for i, c in enumerate(structure):
            field_id = f'dbb_f{i}'
            value = self.form_value(record.get(c['name'])) if key else ''
            p.form_input(field_id, c['name'], hint=f"{c['type']}{', PK' if c['pk'] else ''} - \\N = NULL",
                         id=field_id, value=w.escape(value))
            job['v_' + c['name']] = '#' + field_id
        hint = "Aenderungen speichern" if key else "Datensatz anlegen (leere Felder = Default)"
        p(self.action_btn_parametric("Speichern", job, hint=hint))
        p.pre('', id='result')
