import unittest

from nwebclient import base as b
from nwebclient import runner as r

from nxrunner.projects.gitlab import GitlabProjectApi
from nxrunner.projects.gitlab_report import GitlabReport, cell

ALICE = {'username': 'alice', 'name': 'Alice'}
BOB = {'username': 'bob', 'name': 'Bob'}

ISSUE_NEW = {'iid': 3, 'title': 'Neu | mit Pipe', 'created_at': '2026-11-10T08:00:00.000Z', 'author': ALICE,
             'assignees': [BOB], 'labels': ['TODO'], 'state': 'opened', 'description': '# Kopf\nZeile 2',
             'web_url': 'https://gitlab.example/g/r/-/issues/3'}
ISSUE_OLD = {'iid': 1, 'title': 'Alt', 'created_at': '2026-10-01T08:00:00Z', 'author': BOB, 'assignees': [],
             'labels': [], 'state': 'closed', 'description': '', 'web_url': 'https://gitlab.example/g/r/-/issues/1'}


def label_ev(name, action, time, user=ALICE):
    return {'label': {'name': name}, 'action': action, 'created_at': time, 'user': user}


class FakeReportApi(GitlabProjectApi):
    """ GitLab-API ohne HTTP, Antworten je Endpoint """

    def __init__(self):
        super().__init__('.', [], params={'gitlab_project': 'g/r'})
        self.calls = []
        self.data = {
            '/issues/3/resource_label_events': [label_ev('TODO', 'add', '2026-11-10T08:00:00Z'),
                                                label_ev('TODO', 'remove', '2026-11-11T09:00:00Z'),
                                                label_ev('Doing', 'add', '2026-11-11T09:00:01Z'),
                                                label_ev('Bug', 'add', '2026-11-11T10:00:00Z')],
            '/issues/3/resource_state_events': [],
            '/issues/1/resource_label_events': [label_ev('Done', 'remove', '2026-11-12T10:00:00Z', BOB),
                                                label_ev('Doing', 'add', '2026-09-01T10:00:00Z')],
            '/issues/1/resource_state_events': [{'state': 'closed', 'created_at': '2026-11-12T10:00:02Z',
                                                 'user': BOB}],
        }

    def get(self, endpoint='', **params):
        self.calls.append((endpoint, params))
        if endpoint == '/issues':
            items = [ISSUE_NEW] if 'created_after' in params else [ISSUE_NEW, ISSUE_OLD]
        else:
            items = self.data.get(endpoint, [])
        return items if params.get('page', 1) == 1 else []


class GitlabReportTest(unittest.TestCase):

    def setUp(self):
        self.api = FakeReportApi()
        self.report = GitlabReport(self.api, '2026-11-06', '2026-11-13')

    def test_moves(self):
        moves = self.report.all_moves()
        self.assertEqual([(m['issue']['iid'], m['from'], m['to']) for m in moves],
                         [(3, '-', 'TODO'), (3, 'TODO', 'Doing'), (1, 'Done', 'Closed')])
        self.assertEqual(moves[2]['user'], BOB)

    def test_markdown(self):
        md = self.report.markdown()
        self.assertIn('# GitLab-Report g/r', md)
        self.assertIn('## Neue Issues (1)', md)
        self.assertIn('Neu \\| mit Pipe', md)
        self.assertIn('@bob', md)
        self.assertIn('## Board-Bewegungen (3)', md)
        self.assertIn('| Done | Closed | @bob |', md)
        # Inhalt jedes Issues genau einmal, Beschreibung zitiert
        self.assertEqual(md.count('### #3 '), 1)
        self.assertEqual(md.count('### #1 '), 1)
        self.assertIn('> # Kopf\n> Zeile 2', md)
        self.assertIn('_(keine Beschreibung)_', md)

    def test_period_params(self):
        self.report.created_issues()
        endpoint, params = self.api.calls[0]
        self.assertEqual(params['created_after'], '2026-11-06T00:00:00Z')
        self.assertEqual(params['created_before'], '2026-11-14T00:00:00Z')
        with self.assertRaises(ValueError):
            GitlabReport(self.api, '2026-11-13', '2026-11-01')

    def test_get_all_pages(self):
        api = FakeReportApi()
        api.PER_PAGE = 1
        api.data['/x'] = [{'a': 1}]
        self.assertEqual(api.get_all('/x'), [{'a': 1}, ])
        self.assertEqual([c[1]['page'] for c in api.calls], [1, 2])

    def test_cell(self):
        self.assertEqual(cell('a|b\nc'), 'a\\|b c')

    def test_report_tab(self):
        owner = r.BaseJobExecutor('owner')
        self.api.params['gitlab_token'] = 'x'
        owner.addChild(self.api)
        p = b.Page()
        self.api.part_report(p, {'page': 'gitlab', 'report_from': '2026-11-06', 'report_to': '2026-11-13'})
        html = str(p)
        self.assertIn('type="date"', html)
        self.assertIn('name="page"', html)
        self.assertIn('gitlab_report_md', html)
        self.assertIn('Board-Bewegungen', html)

    def test_report_job(self):
        res = self.api.report_job('2026-11-06', '2026-11-13')
        self.assertTrue(res['success'], res)
        self.assertIn('## Neue Issues', res['markdown'])


if __name__ == '__main__':
    unittest.main()
