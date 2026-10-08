"""
phpMyAdmin-Client mit DB-API 2.0 (PEP 249) kompatibler Schnittstelle.

    from nxrunner.apis import phpmyadmin

    con = phpmyadmin.connect("https://example.com/phpmyadmin", "user", "pass", "mydb")
    cur = con.cursor()
    cur.execute("SELECT * FROM users WHERE id = %s", (1,))
    print(cur.fetchall())
    con.close()
"""
import datetime
import decimal
import re
import requests
from bs4 import BeautifulSoup
import json
import os


# --- DB-API Modul-Attribute ---------------------------------------------------

apilevel = "2.0"
threadsafety = 1  # Modul teilbar, Verbindungen nicht
paramstyle = "pyformat"  # %s und %(name)s


# --- DB-API Exceptions --------------------------------------------------------

class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# --- DB-API Typ-Objekte und Konstruktoren -------------------------------------

Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime


def DateFromTicks(ticks):
    return Date.fromtimestamp(ticks)


def TimeFromTicks(ticks):
    return Timestamp.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks):
    return Timestamp.fromtimestamp(ticks)


def Binary(value):
    return bytes(value)


class _DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values

    def __hash__(self):
        return hash(self.values)


# phpMyAdmin liefert keine Typinformationen, daher werden Python-Typen verwendet
STRING = _DBAPITypeObject(str)
BINARY = _DBAPITypeObject(bytes)
NUMBER = _DBAPITypeObject(int, float, decimal.Decimal)
DATETIME = _DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
ROWID = _DBAPITypeObject(int)


# --- Parameter-Escaping -------------------------------------------------------

_ESCAPE_MAP = {
    "\0": "\\0",
    "\n": "\\n",
    "\r": "\\r",
    "\x1a": "\\Z",
    "'": "\\'",
    '"': '\\"',
    "\\": "\\\\",
}


def escape(value):
    """Wandelt einen Python-Wert in ein SQL-Literal um."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "X'" + bytes(value).hex() + "'"
    if isinstance(value, datetime.datetime):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, (datetime.date, datetime.time)):
        return "'" + value.isoformat() + "'"
    if isinstance(value, (list, tuple, set, frozenset)):
        return "(" + ", ".join(escape(v) for v in value) + ")"
    return "'" + "".join(_ESCAPE_MAP.get(c, c) for c in str(value)) + "'"


def format_query(operation, parameters=None):
    """Setzt Parameter im pyformat-Stil (%s / %(name)s) in die Abfrage ein."""
    if parameters is None:
        return operation
    try:
        if isinstance(parameters, dict):
            return operation % {k: escape(v) for k, v in parameters.items()}
        return operation % tuple(escape(v) for v in parameters)
    except (TypeError, KeyError, ValueError) as e:
        raise ProgrammingError(f"Parameter passen nicht zur Abfrage: {e}") from e


# --- Cursor -------------------------------------------------------------------

class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.arraysize = 1
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows = []
        self._pos = 0
        self._closed = False

    def _check(self):
        if self._closed:
            raise InterfaceError("Cursor ist geschlossen")
        self.connection._check()

    def close(self):
        self._closed = True
        self._rows = []

    def execute(self, operation, parameters=None):
        self._check()
        sql = format_query(operation, parameters)
        columns, rows, affected = self.connection._query(sql)
        self._rows = rows
        self._pos = 0
        if columns:
            self.description = tuple((name, None, None, None, None, None, None) for name in columns)
            self.rowcount = len(rows)
        else:
            self.description = None
            self.rowcount = affected if affected is not None else -1
        return self

    def executemany(self, operation, seq_of_parameters):
        self._check()
        total = 0
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            if self.rowcount > 0:
                total += self.rowcount
        self.rowcount = total
        self.description = None
        self._rows = []
        return self

    def fetchone(self):
        self._check_result()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size=None):
        self._check_result()
        if size is None:
            size = self.arraysize
        rows = self._rows[self._pos:self._pos + size]
        self._pos += len(rows)
        return rows

    def fetchall(self):
        self._check_result()
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

    def _check_result(self):
        self._check()
        if self.description is None:
            raise ProgrammingError("Keine Ergebnismenge vorhanden")

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def __iter__(self):
        return iter(self.fetchone, None)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# --- Connection ---------------------------------------------------------------

class PhpMyAdminClient:
    """DB-API 2.0 Connection über die phpMyAdmin-Weboberfläche.

    phpMyAdmin arbeitet im Autocommit-Modus; commit() ist daher wirkungslos
    und rollback() wird nicht unterstützt.
    """

    # Exceptions als Connection-Attribute (optionale DB-API Erweiterung)
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, base_url, username, password, database=None):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.database = database
        self.session = requests.Session()
        self.token = None
        self._closed = False

    def login(self):
        # Login-Seite laden
        r = self.session.get(self.base_url)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        token_input = soup.find("input", {"name": "token"})
        token = token_input["value"] if token_input else ""

        data = {
            "pma_username": self.username,
            "pma_password": self.password,
            "server": 1,
            "target": "index.php",
            "token": token
        }

        r = self.session.post(
            f"{self.base_url}/index.php",
            data=data,
            allow_redirects=True
        )
        r.raise_for_status()

        self.token = self._extract_token(r.text)

        if not self.token:
            raise OperationalError("Login fehlgeschlagen oder Token nicht gefunden")

        return True

    # --- DB-API Connection-Methoden ---

    def _check(self):
        if self._closed:
            raise InterfaceError("Verbindung ist geschlossen")

    def cursor(self):
        self._check()
        return Cursor(self)

    def commit(self):
        self._check()

    def rollback(self):
        self._check()
        raise NotSupportedError("phpMyAdmin unterstützt keine Transaktionen über mehrere Requests")

    def close(self):
        if not self._closed:
            self.session.close()
            self.token = None
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # --- Abfragen ---

    def _request(self, sql, database=None):
        self._check()
        if self.token is None:
            self.login()
        if database is None:
            database = self.database
        payload = {
            "db": database,
            "sql_query": sql,
            "token": self.token,
            "ajax_request": True,
            "ajax_page_request": True,
            "session_max_rows": "all",  # keine Paginierung
            "pftext": "F",  # Texte nicht kürzen
        }
        try:
            r = self.session.post(
                f"{self.base_url}/index.php?route=/import",
                data=payload
            )
            r.raise_for_status()
        except requests.RequestException as e:
            raise OperationalError(str(e)) from e
        try:
            data = json.loads(r.text)
        except ValueError as e:
            raise InterfaceError("Ungültige Antwort von phpMyAdmin") from e
        if not data.get("success", True):
            error = data.get("error") or data.get("message") or "Unbekannter Fehler"
            raise ProgrammingError(BeautifulSoup(str(error), "html.parser").get_text(" ", strip=True))
        return data

    def _query(self, sql, database=None):
        """Führt SQL aus und liefert (Spaltennamen, Zeilen als Tupel, betroffene Zeilen)."""
        data = self._request(sql, database)
        columns, rows = self._parse_table(data)
        affected = None
        match = re.search(r"(\d+)\s+(?:rows?|Zeilen?|Datens[äa]tze?)\b", BeautifulSoup(
            data.get("message") or "", "html.parser").get_text(" "))
        if match:
            affected = int(match.group(1))
        return columns, [tuple(row) for row in rows], affected

    def execute_sql(self, sql, database=None):
        data = self._request(sql, database)
        return self.extract_table(data)

    def extract_table(self, response: dict) -> list[dict]:
        headers, rows = self._parse_table(response)
        return [dict(zip(headers, row)) for row in rows]

    def _parse_table(self, response: dict):
        html = response.get("message") or ""
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table", class_="table_results")
        if not table:
            return None, []

        # Spaltennamen
        headers = [
            th.get_text(strip=True)
            for th in table.select("thead th[data-column]")
        ]

        result = []

        for tr in table.select("tbody tr"):
            # nur Datenzellen, nicht die Bearbeiten/Kopieren/Löschen-Spalten
            cells = [td for td in tr.find_all("td") if "data" in (td.get("class") or [])]
            if len(cells) != len(headers):
                cells = tr.find_all("td")[-len(headers):] if headers else []

            row = []
            for cell in cells:
                row.append(self._convert(cell))

            result.append(row)

        return headers, result

    @staticmethod
    def _convert(cell):
        if "null" in (cell.get("class") or []):
            return None

        value = cell.get_text(strip=True)

        # numerische Konvertierung
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    def _extract_token(self, html):
        match = re.search(r'token=([a-zA-Z0-9%]+)', html)
        if match:
            return match.group(1)

        soup = BeautifulSoup(html, "html.parser")
        token_input = soup.find("input", {"name": "token"})
        if token_input:
            return token_input.get("value")

        return None


Connection = PhpMyAdminClient


def connect(base_url=None, user=None, password=None, database=None, **kwargs):
    """DB-API connect(); akzeptiert auch host/url/username/passwd/db als Aliase."""
    base_url = base_url or kwargs.pop("url", None) or kwargs.pop("host", None)
    user = user or kwargs.pop("username", None)
    password = password or kwargs.pop("passwd", None)
    database = database or kwargs.pop("db", None)
    if not base_url:
        raise InterfaceError("base_url fehlt")
    return PhpMyAdminClient(base_url, user, password, database)


if __name__ == "__main__":
    with connect(
        os.environ.get("DB_URL", "https://example.com/phpmyadmin"),
        os.environ.get("DB_USER"),
        os.environ.get("DB_PASS"),
        os.environ.get("DB_NAME"),
    ) as con:
        cur = con.cursor()
        cur.execute(os.environ.get("SQL", "SELECT * FROM users LIMIT 10"))
        print([d[0] for d in cur.description])
        for row in cur:
            print(row)
