"""
Markdown-Report eines GitLab-Projekts fuer einen Zeitraum:

- neu erstellte Issues mit Zuweisungen
- Verschiebungen auf dem Issue-Board zwischen den Listen TODO, Doing, Done und Closed
  (aus resource_label_events und resource_state_events)
- einmalig der Inhalt jedes im Report vorkommenden Issues
"""
import datetime
from typing import List, Optional

BOARD_LABELS = ('TODO', 'Doing', 'Done', 'Closed')
OPEN, CLOSED = 'Open', 'Closed'
MERGE_SECONDS = 5  # Ereignisse desselben Benutzers innerhalb dieser Zeit gelten als eine Verschiebung


def parse_time(value: str) -> Optional[datetime.datetime]:
    """ GitLab-Zeitstempel (ISO 8601, z.B. 2026-11-10T12:34:56.789Z) als aware datetime """
    if not value:
        return None
    dt = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=datetime.timezone.utc)


def parse_date(value) -> datetime.date:
    if isinstance(value, datetime.date):
        return value
    return datetime.date.fromisoformat(str(value).strip()[:10])


def cell(value) -> str:
    """ Text fuer eine Markdown-Tabellenzelle """
    return str(value if value is not None else '').replace('|', '\\|').replace('\r', '').replace('\n', ' ')


def user_name(user: Optional[dict]) -> str:
    if not user:
        return '-'
    return f"@{user['username']}" if user.get('username') else user.get('name', '-')


def assignees(issue: dict) -> str:
    users = issue.get('assignees') or ([issue['assignee']] if issue.get('assignee') else [])
    return ', '.join(user_name(u) for u in users) or '-'


class GitlabReport:
    """
    Report fuer [since, until] (beide Tage einschliesslich, UTC).

    :param api: GitlabProjectApi (benoetigt get_all und project_path)
    """

    def __init__(self, api, since, until, board_labels=BOARD_LABELS):
        self.api = api
        self.since = parse_date(since)
        self.until = parse_date(until)
        if self.until < self.since:
            raise ValueError(f"Ende {self.until} liegt vor Beginn {self.since}")
        self.board_labels = {lbl.lower(): lbl for lbl in board_labels}
        utc = datetime.timezone.utc
        self.start = datetime.datetime.combine(self.since, datetime.time.min, tzinfo=utc)
        self.end = datetime.datetime.combine(self.until + datetime.timedelta(days=1), datetime.time.min, tzinfo=utc)

    @staticmethod
    def iso(dt: datetime.datetime) -> str:
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def in_period(self, value: str) -> bool:
        dt = parse_time(value)
        return dt is not None and self.start <= dt < self.end

    # --- Daten --------------------------------------------------------------

    def created_issues(self) -> List[dict]:
        issues = self.api.get_all('/issues', scope='all', created_after=self.iso(self.start),
                                  created_before=self.iso(self.end), order_by='created_at', sort='asc')
        return [i for i in issues if self.in_period(i.get('created_at'))]

    def updated_issues(self) -> List[dict]:
        """ Kandidaten fuer Board-Bewegungen: alle seit Beginn geaenderten Issues """
        return self.api.get_all('/issues', scope='all', updated_after=self.iso(self.start))

    def board_events(self, iid: int) -> List[dict]:
        """
        Label- und Statusereignisse im Zeitraum, die das Board betreffen, normalisiert zu
        {'time', 'user', 'add': [Liste], 'remove': [Liste]}, zeitlich sortiert
        """
        res = []
        for ev in self.api.get_all(f'/issues/{iid}/resource_label_events'):
            name = (ev.get('label') or {}).get('name', '')
            if name.lower() in self.board_labels and self.in_period(ev.get('created_at')):
                key = 'add' if ev.get('action') == 'add' else 'remove'
                res.append({'time': parse_time(ev['created_at']), 'user': ev.get('user'),
                            key: [self.board_labels[name.lower()]]})
        for ev in self.api.get_all(f'/issues/{iid}/resource_state_events'):
            if ev.get('state') in ('closed', 'reopened') and self.in_period(ev.get('created_at')):
                closed = ev['state'] == 'closed'
                res.append({'time': parse_time(ev['created_at']), 'user': ev.get('user'),
                            'add': [CLOSED if closed else OPEN], 'remove': [] if closed else [CLOSED]})
        return sorted(res, key=lambda e: e['time'])

    @staticmethod
    def merge_events(events: List[dict]) -> List[dict]:
        """ fasst zusammengehoerige Ereignisse (Label entfernen + Label setzen) zu einer Verschiebung zusammen """
        res = []
        for ev in events:
            last = res[-1] if res else None
            if (last is not None and user_name(last['user']) == user_name(ev['user'])
                    and (ev['time'] - last['time']).total_seconds() <= MERGE_SECONDS):
                last['add'] = last['add'] + ev.get('add', [])
                last['remove'] = last['remove'] + ev.get('remove', [])
            else:
                res.append({'time': ev['time'], 'user': ev['user'],
                            'add': list(ev.get('add', [])), 'remove': list(ev.get('remove', []))})
        return res

    def moves(self, issue: dict) -> List[dict]:
        """ Verschiebungen eines Issues: {'issue', 'time', 'user', 'from', 'to'} """
        res = []
        for ev in self.merge_events(self.board_events(issue['iid'])):
            to = [lbl for lbl in ev['add'] if lbl not in ev['remove']]
            frm = [lbl for lbl in ev['remove'] if lbl not in ev['add']]
            if to or frm:
                res.append({'issue': issue, 'time': ev['time'], 'user': ev['user'],
                            'from': ', '.join(frm) or '-', 'to': ', '.join(to) or OPEN})
        return res

    def all_moves(self) -> List[dict]:
        moves = [m for issue in self.updated_issues() for m in self.moves(issue)]
        return sorted(moves, key=lambda m: m['time'])

    # --- Markdown -----------------------------------------------------------

    def markdown(self) -> str:
        created = self.created_issues()
        moves = self.all_moves()
        issues = {i['iid']: i for i in created}
        for m in moves:
            issues.setdefault(m['issue']['iid'], m['issue'])
        parts = [self.md_head(), self.md_created(created), self.md_moves(moves),
                 self.md_contents([issues[k] for k in sorted(issues)])]
        return '\n\n'.join(parts) + '\n'

    def md_head(self) -> str:
        return (f"# GitLab-Report {self.api.project_path() or ''}\n\n"
                f"Zeitraum: {self.since.isoformat()} bis {self.until.isoformat()} (UTC)  \n"
                f"Erstellt: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")

    @staticmethod
    def md_issue_ref(issue: dict) -> str:
        return f"[#{issue['iid']}]({issue.get('web_url', '#')})"

    def md_created(self, created: List[dict]) -> str:
        lines = [f"## Neue Issues ({len(created)})", '']
        if not created:
            return '\n'.join(lines + ['Keine neuen Issues im Zeitraum.'])
        lines += ['| Issue | Titel | Erstellt | Von | Zugewiesen | Labels |', '|---|---|---|---|---|---|']
        for i in created:
            lines.append(f"| {self.md_issue_ref(i)} | {cell(i.get('title'))} | {cell(i.get('created_at', '')[:10])} "
                         f"| {cell(user_name(i.get('author')))} | {cell(assignees(i))} "
                         f"| {cell(', '.join(i.get('labels', [])) or '-')} |")
        return '\n'.join(lines)

    def md_moves(self, moves: List[dict]) -> str:
        lines = [f"## Board-Bewegungen ({len(moves)})", '',
                 f"Listen: {', '.join(self.board_labels.values())}", '']
        if not moves:
            return '\n'.join(lines + ['Keine Verschiebungen im Zeitraum.'])
        lines += ['| Zeit (UTC) | Issue | Titel | Von | Nach | Durch |', '|---|---|---|---|---|---|']
        for m in moves:
            lines.append(f"| {m['time'].strftime('%Y-%m-%d %H:%M')} | {self.md_issue_ref(m['issue'])} "
                         f"| {cell(m['issue'].get('title'))} | {cell(m['from'])} | {cell(m['to'])} "
                         f"| {cell(user_name(m['user']))} |")
        return '\n'.join(lines)

    def md_contents(self, issues: List[dict]) -> str:
        """ Inhalt jedes Issues genau einmal, Beschreibung als Zitat (Ueberschriften darin stoeren nicht) """
        lines = ['## Inhalt der Issues']
        if not issues:
            return '\n\n'.join(lines + ['Keine Issues im Zeitraum.'])
        for i in issues:
            lines.append(f"### #{i['iid']} {i.get('title', '')}")
            lines.append(f"Status: {i.get('state', '-')} · Labels: {', '.join(i.get('labels', [])) or '-'} · "
                         f"Zugewiesen: {assignees(i)} · {self.md_issue_ref(i)}")
            desc = (i.get('description') or '').strip()
            lines.append('\n'.join('> ' + line for line in desc.splitlines()) if desc else '_(keine Beschreibung)_')
        return '\n\n'.join(lines)
