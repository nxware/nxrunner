"""
    Freigabe von MCP-Aufrufen: Aufrufe von Tools mit approve: true warten, bis sie auf /sys/mcp genehmigt
    oder abgelehnt werden (oder bis zum Timeout)

    Jeder wartende Aufruf belegt einen Thread des Webservers (waitress: standardmaessig 4), daher duerfen
    hoechstens max_pending Aufrufe gleichzeitig warten, weitere werden sofort abgewiesen.
"""
import datetime
import itertools
import threading

APPROVED = 'genehmigt'
DENIED = 'abgelehnt'
TIMEOUT = 'Zeitueberschreitung'
REJECTED = 'abgewiesen (zu viele offene Freigaben)'


class Approval:
    """ ein MCP-Aufruf, der auf Freigabe wartet """

    def __init__(self, approval_id, tool, arguments, client=''):
        self.id = approval_id
        self.tool = tool
        self.arguments = dict(arguments)
        self.client = client
        self.created = datetime.datetime.now()
        self.decided = None
        self.decision = None
        self.event = threading.Event()

    @property
    def approved(self) -> bool:
        return self.decision == APPROVED

    def to_dict(self) -> dict:
        return {'id': self.id, 'tool': self.tool, 'arguments': self.arguments, 'client': self.client,
                'created': self.created.isoformat(timespec='seconds'), 'decision': self.decision,
                'decided': self.decided.isoformat(timespec='seconds') if self.decided else None}


class McpApprovals:
    """ thread-sichere Warteschlange der offenen Freigaben und Verlauf der Entscheidungen """

    def __init__(self, max_pending=2, history_size=20):
        self.max_pending = int(max_pending)
        self.history_size = int(history_size)
        self.lock = threading.Lock()
        self.ids = itertools.count(1)
        self._pending = {}
        self._history = []

    def request(self, tool, arguments, client='', timeout=120.0) -> Approval:
        """ wartet (blockierend) auf die Entscheidung, Ergebnis in Approval.decision """
        with self.lock:
            approval = Approval(next(self.ids), tool, arguments, client)
            if len(self._pending) >= self.max_pending:
                self._finish(approval, REJECTED)
                return approval
            self._pending[approval.id] = approval
        approval.event.wait(float(timeout))
        with self.lock:
            if self._pending.pop(approval.id, None) is not None:
                self._finish(approval, approval.decision or TIMEOUT)
        return approval

    def decide(self, approval_id, approve: bool) -> bool:
        """ :return: False, wenn die Anfrage nicht (mehr) offen ist """
        with self.lock:
            approval = self._pending.get(int(approval_id))
            if approval is None or approval.decision is not None:
                return False
            approval.decision = APPROVED if approve else DENIED
            approval.decided = datetime.datetime.now()
        approval.event.set()
        return True

    def _finish(self, approval: Approval, decision):
        """ unter self.lock aufrufen """
        approval.decision = decision
        approval.decided = approval.decided or datetime.datetime.now()
        self._history.insert(0, approval)
        del self._history[self.history_size:]

    def pending(self) -> list:
        """ offene Anfragen ohne Entscheidung, aelteste zuerst """
        with self.lock:
            return sorted((a for a in self._pending.values() if a.decision is None), key=lambda a: a.id)

    def open_count(self) -> int:
        return len(self.pending())

    def history(self) -> list:
        with self.lock:
            return list(self._history)
