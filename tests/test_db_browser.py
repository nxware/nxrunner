"""
Unittests fuer nxrunner.db.browser (mit SQLite im Speicher)

    python -m unittest tests.test_db_browser
"""
import json
import sqlite3
import unittest

from nwebclient import base as b

from nxrunner.db.browser import DbBrowser, connect, ROWID


def create_db():
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn.executescript("""
        CREATE TABLE person (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);
        CREATE TABLE tag (label TEXT);
        CREATE VIEW adults AS SELECT * FROM person WHERE age >= 18;
        CREATE INDEX ix_person_name ON person(name);
        INSERT INTO person (name, age) VALUES ('Anna', 30), ('Bob', 12), ('Cem', NULL);
        INSERT INTO tag VALUES ('a'), ('a'), ('b');
    """)
    conn.commit()
    return conn


class DbBrowserTest(unittest.TestCase):

    def setUp(self):
        self.db = DbBrowser(create_db())

    def test_connect_str(self):
        self.assertEqual(DbBrowser('sqlite:///:memory:').dialect.name, 'sqlite')
        self.assertIsInstance(connect(':memory:'), sqlite3.Connection)

    def test_objects(self):
        objs = {o['name']: o['type'] for o in self.db.list_objects()}
        self.assertEqual(objs['person'], 'table')
        self.assertEqual(objs['adults'], 'view')
        self.assertEqual(objs['ix_person_name'], 'index')

    def test_structure(self):
        cols = self.db.get_structure('person')
        self.assertEqual([c['name'] for c in cols], ['id', 'name', 'age'])
        self.assertTrue(cols[0]['pk'])
        self.assertTrue(cols[1]['notnull'])
        self.assertEqual(self.db.key_columns('person'), ['id'])
        self.assertEqual(self.db.key_columns('tag'), [ROWID])

    def test_unknown_table(self):
        res = self.db.execute(dict(op='structure', table='person; DROP TABLE person'))
        self.assertFalse(res['success'])

    def test_sql_op(self):
        res = self.db.execute(dict(op='sql', sql='SELECT name FROM person ORDER BY id'))
        self.assertTrue(res['success'], res)
        self.assertEqual(res['rows'], [['Anna'], ['Bob'], ['Cem']])
        res = self.db.execute(dict(op='sql', sql="UPDATE person SET age = 1 WHERE name = 'Bob'"))
        self.assertEqual(res['rowcount'], 1)
        self.assertFalse(self.db.execute(dict(op='sql', sql='SELECT * FROM nix'))['success'])

    def test_update_row(self):
        res = self.db.execute(dict(op='update_row', table='person', key=json.dumps({'id': 2}),
                                   v_name='Bobby', v_age='\\N'))
        self.assertTrue(res['success'], res)
        _, rows, _ = self.db.query('SELECT name, age FROM person WHERE id = 2')
        self.assertEqual(rows, [['Bobby', None]])

    def test_update_unknown_column(self):
        res = self.db.execute(dict(op='update_row', table='person', key='{"id": 1}', v_evil='x'))
        self.assertFalse(res['success'])

    def test_delete_row_rowid(self):
        cols, rows = self.db.select_rows('tag')
        rowid = rows[0][cols.index(ROWID)]
        res = self.db.execute(dict(op='delete_row', table='tag', key=json.dumps({ROWID: rowid})))
        self.assertTrue(res['success'], res)
        self.assertEqual(self.db.count_rows('tag'), 2)

    def test_delete_multiple_rolls_back(self):
        # ohne rowid trifft label='a' zwei Zeilen -> Rollback
        res = self.db.execute(dict(op='delete_row', table='tag', key='{"label": "a"}'))
        self.assertFalse(res['success'])
        self.assertEqual(self.db.count_rows('tag'), 3)

    def test_delete_null_key(self):
        res = self.db.execute(dict(op='delete_row', table='person', key='{"age": null}'))
        self.assertTrue(res['success'], res)
        self.assertEqual(self.db.count_rows('person'), 2)

    def test_insert_row(self):
        res = self.db.execute(dict(op='insert_row', table='person', v_name='Dora', v_age='40', v_id=''))
        self.assertTrue(res['success'], res)
        self.assertEqual(self.db.count_rows('person'), 4)

    def test_pages_render(self):
        for part, params in [(self.db.part_index, {'sql': 'SELECT * FROM person'}),
                             (self.db.part_table, {'table': 'person'}),
                             (self.db.part_table, {'table': 'tag'}),
                             (self.db.part_table, {'table': 'adults'}),
                             (self.db.part_edit, {'table': 'person', 'key': '{"id": 1}'}),
                             (self.db.part_edit, {'table': 'person'})]:
            p = b.Page()
            part(p, params)
            self.assertNotIn('Error', str(p), f"{part.__name__} {params}")
        p = b.Page()
        self.db.part_table(p, {'table': 'person'})
        self.assertIn('Anna', str(p))
        self.assertIn('delete_row', str(p))


if __name__ == '__main__':
    unittest.main()
