"""
    Freigabe von MCP-Aufrufen: Jobs von Tools mit approve: true warten, bis sie auf /sys/mcp/pending genehmigt
    oder abgelehnt werden (oder bis zum Timeout). Nach der Ausfuehrung wird das Ergebnis am Job gespeichert.

    Jeder wartende Aufruf belegt einen Thread des Webservers (waitress: standardmaessig 4), daher duerfen
    hoechstens max_pending Aufrufe gleichzeitig warten, weitere werden sofort abgewiesen.
"""
import datetime
import html
import itertools
import json
import threading

from nwebclient import web as w

APPROVED = 'genehmigt'
DENIED = 'abgelehnt'
TIMEOUT = 'Zeitueberschreitung'
REJECTED = 'abgewiesen (zu viele offene Freigaben)'

BOX_STYLE = 'border: 1px solid #8886; border-radius: 6px; padding: 8px; margin: 8px 0;'
PRE_STYLE = 'margin: 0; max-height: 300px; overflow: auto; white-space: pre-wrap;'


class Approval:
    """ ein Job eines MCP-Aufrufs, der auf Freigabe wartet """

    def __init__(self, approval_id, tool, job, client=''):
        self.id = approval_id
        self.tool = tool
        self.job = dict(job)
        self.client = client
        self.created = datetime.datetime.now()
        self.decided = None
        self.decision = None
        self.result = None  # Ergebnis des Jobs, sobald er ausgefuehrt wurde
        self.executed = None
        self.event = threading.Event()

    @property
    def approved(self) -> bool:
        return self.decision == APPROVED

    def to_dict(self) -> dict:
        return {'id': self.id, 'tool': self.tool, 'job': self.job, 'client': self.client,
                'created': self.created.isoformat(timespec='seconds'), 'decision': self.decision,
                'decided': self.decided.isoformat(timespec='seconds') if self.decided else None,
                'result': self.result}


class McpApprovals:
    """ thread-sichere Warteschlange der offenen Freigaben und Verlauf der Entscheidungen """

    def __init__(self, max_pending=2, history_size=20):
        self.max_pending = int(max_pending)
        self.history_size = int(history_size)
        self.lock = threading.Lock()
        self.ids = itertools.count(1)
        self._pending = {}
        self._history = []

    def request(self, tool, job, client='', timeout=120.0) -> Approval:
        """ wartet (blockierend) auf die Entscheidung, Ergebnis in Approval.decision """
        with self.lock:
            approval = Approval(next(self.ids), tool, job, client)
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

    def set_result(self, approval: Approval, result):
        """ Ergebnis des ausgefuehrten Jobs speichern (erscheint im Verlauf) """
        with self.lock:
            approval.result = result
            approval.executed = datetime.datetime.now()

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


# --- HTML -------------------------------------------------------------------------------------

def value_html(value) -> str:
    """ Wert einer Tabellenzelle: verschachtelte Objekte/Listen als JSON, Text unveraendert, sonst JSON-Literal """
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, indent=2, ensure_ascii=False, default=str)
        return w.tag('pre', html.escape(text), style=PRE_STYLE)
    if isinstance(value, str):
        return w.tag('span', html.escape(value), style='white-space: pre-wrap;')
    return w.tag('code', html.escape(json.dumps(value, default=str)))


def kv_table(data) -> str:
    """ Tabelle Schluessel | Wert eines JSON-Objekts, andere Werte als JSON """
    if not isinstance(data, dict):
        return value_html(data)
    if not data:
        return w.tag('div', '(leer)')
    head = w.tr(w.tag('th', 'Schl&uuml;ssel') + w.tag('th', 'Wert'))
    rows = ''.join(w.tr(w.td(w.tag('b', html.escape(str(k)))) + w.td(value_html(v))) for k, v in data.items())
    return w.table(head + rows)


def decide_buttons(a: Approval) -> str:
    return (w.tag('button', 'Genehmigen', type='button', onclick=f"mcpDecide('approve', {a.id})",
                  title=f'Job von {html.escape(a.tool)} genau so jetzt ausfuehren') + ' ' +
            w.tag('button', 'Ablehnen', type='button', onclick=f"mcpDecide('deny', {a.id})",
                  title='Job nicht ausfuehren, der MCP-Client erhaelt einen Fehler'))


def approval_status(a: Approval) -> str:
    if a.decision is None:
        return 'wartet auf Freigabe'
    if a.executed is not None:
        return f'{a.decision}, ausgefuehrt {a.executed.strftime("%H:%M:%S")}'
    return a.decision + (', wird ausgefuehrt' if a.approved else '')


def approval_card(a: Approval) -> str:
    """ ein Job mit Kopfzeile, Schluessel/Wert-Tabelle, Buttons (offen) bzw. Ergebnis (ausgefuehrt) """
    head = (f'<b>#{a.id} {html.escape(a.tool)}</b> &middot; {a.created.strftime("%H:%M:%S")}'
            f'{" &middot; " + html.escape(a.client) if a.client else ""} &middot; {html.escape(approval_status(a))}')
    body = w.tag('div', head) + w.tag('h4', 'Job') + kv_table(a.job)
    if a.decision is None:
        body += w.tag('div', decide_buttons(a), style='margin-top: 6px;')
    if a.executed is not None:
        body += w.tag('h4', 'Ergebnis') + kv_table(a.result)
    return w.tag('div', body, style=BOX_STYLE)


def approvals_html(approvals: McpApprovals, message='', history_count=10) -> str:
    """ offene Jobs und die letzten Entscheidungen (mit Ergebnis) """
    pending, history = approvals.pending(), approvals.history()[:history_count]
    res = w.tag('h3', f'Offene Freigaben ({len(pending)})')
    if message:
        res += w.alert_info(html.escape(message))
    res += ''.join(approval_card(a) for a in pending) or w.tag('div', 'Keine offenen Freigaben.')
    if history:
        res += w.tag('h3', 'Letzte Entscheidungen') + ''.join(approval_card(a) for a in history)
    return res
