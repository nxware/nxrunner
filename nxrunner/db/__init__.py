"""
  @see https://github.com/jwills/buenavista/blob/main/buenavista/examples/postgres_proxy.py
"""
import os
from typing import List, Dict, Any, Tuple, Iterator

from buenavista.postgres import BuenaVistaServer
from buenavista.backends.postgres import PGConnection, PGSession
from buenavista.core import BVType, Connection, QueryResult, Session


class DictListQueryResult(QueryResult):
    def __init__(self, data: List[Dict[str, Any]]):
        super().__init__()
        self._data = data
        self._fields = list(data[0].keys()) if data else []
        # Wir setzen das Format auf 0 (Text) oder 1 (Binary) - Standard ist 0
        self.result_format = 0

    def has_results(self) -> bool:
        return True

    def column_count(self) -> int:
        return len(self._fields)

    def column(self, index: int) -> Tuple[str, BVType]:
        name = self._fields[index]
        # Einfache Typerkennung basierend auf dem ersten Datensatz
        sample_val = self._data[0][name] if self._data else None

        if isinstance(sample_val, int):
            bv_type = BVType.INTEGER
        elif isinstance(sample_val, float):
            bv_type = BVType.FLOAT
        elif isinstance(sample_val, bool):
            bv_type = BVType.BOOL
        else:
            bv_type = BVType.TEXT

        return name, bv_type

    def rows(self) -> Iterator[List[Any]]:
        for row_dict in self._data:
            # Wir extrahieren die Werte in der Reihenfolge der Felder
            yield [row_dict.get(f) for f in self._fields]

    def status(self) -> str:
        # Typischer Postgres-Status für SELECT-Abfragen
        return f"SELECT {len(self._data)}"


class BaseConnection(Connection):
    """Translation layer from an upstream data source into the BV representation of a query result."""

    class BaseSession(Session):
        def __init__(self, owner=None):
            super().__init__()
            self.owner = owner
        def cursor(self):
            raise NotImplementedError
        def close(self):
            pass
        def execute_sql(self, sql: str, params=None) -> QueryResult:
            return self.owner.execute_sql(sql, params)
        def in_transaction(self) -> bool:
            return False
        def load_df_function(self, table: str):
            raise NotImplementedError

    def __init__(self):
        super().__init__()

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        return None

    def new_session(self) -> Session:
        return self.BaseSession(self)

    def parameters(self) -> Dict[str, str]:
        return {}


class NxSession(PGSession):
    def __init__(self, parent, conn, owner=None):
        super().__init__(parent, conn)
        self.owner = owner

    def execute_sql(self, sql: str, params=None) -> QueryResult:
        if 'dummy' in sql:
            return DictListQueryResult([
                {'title': 'dummy'},
                {'title': 'dum'}
            ])
        else:
            print(sql)
            return super().execute_sql(sql, params)


class NxConnection(PGConnection):
    def __init__(self, conninfo, **kwargs):
        super().__init__(conninfo, **kwargs)

    def new_session(self) -> Session:
        conn = self.pool.getconn()
        conn.autocommit = True
        return NxSession(self, conn, self)


address = ("localhost", 5433)
server = BuenaVistaServer(
    address,
    NxConnection(
        conninfo="",
        host="localhost",
        port=5432,
        user='postgres',
        password='password',
        dbname="postgres"
    ),
)
ip, port = server.server_address
print(f"Listening on {ip}:{port}")
server.serve_forever()