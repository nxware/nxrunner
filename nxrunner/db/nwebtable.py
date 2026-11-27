"""
Gemeinsamer Zugriff von Runnern auf die nweb-Persistence (Arg DB) des Root-Runners.
"""

from nwebclient import base


def root_nweb(runner):
    """ nweb-Persistence (Arg DB) des Root-Runners von runner oder None """
    root = runner.getRoot()
    nw = getattr(root, 'nweb', None) if root is not None else None
    return nw if nw is not None and hasattr(nw, 'dml') else None


def dbapi_connection(nw):
    """
      DB-API-2.0-Verbindung einer nweb-Persistence: nweb.DB haelt sie in connection,
      bei NWeb(cfg, DB(...)) ist connection wiederum ein DB-Objekt. nweb.DB hat zwar cursor(),
      aber kein commit/rollback, daher wird entpackt, solange es ein Attribut connection gibt.
    """
    conn = nw
    for _ in range(5):  # Schutz gegen zyklische Verweise
        inner = getattr(conn, 'connection', None)
        if inner is None or inner is conn:
            break
        conn = inner
    return conn


class NwebTableMixin(base.Base):
    """
      Mixin fuer Runner mit eigenen Tabellen in der nweb-Persistence.
      Erbt von base.Base, damit getRoot() garantiert vorhanden ist.

      nweb() liefert die Persistence des Root-Runners und legt beim ersten Zugriff per create_table(nw)
      die Tabellen an. Ohne Persistence wirft nweb() einen RuntimeError, mit NWEB_REQUIRED = False
      liefert es stattdessen None (dann z.B. nur im Speicher arbeiten).
    """

    NWEB_REQUIRED = True

    table_created = False

    def nweb_available(self) -> bool:
        """ True, wenn der Root-Runner eine nweb-Persistence mit dml hat """
        return self.nweb_raw() is not None

    def nweb_raw(self):
        """ nweb-Persistence des Root-Runners (ohne Tabellen anzulegen) oder None """
        return root_nweb(self)

    def nweb(self):
        nw = self.nweb_raw()
        if nw is None:
            if self.NWEB_REQUIRED:
                raise RuntimeError('nweb-Persistence nicht verfuegbar (Arg DB setzen)')
            return None
        if not self.table_created:
            self.create_table(nw)
            self.table_created = True
        return nw

    def create_table(self, nw):
        """ Tabellen anlegen (CREATE TABLE IF NOT EXISTS ...), wird einmal pro Instanz aufgerufen """
