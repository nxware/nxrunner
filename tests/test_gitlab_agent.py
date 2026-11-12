import unittest

from nwebclient import runner as r
from nwebclient.runner import op
from nwebclient.runner.dispatcher import LazyDispatcher

from nwebclient import base as b

from nxrunner.projects.gitlab import GitlabProjectApi, GitlabAgentRunner


class FakeAgent(r.BaseJobExecutor):
    """ Runner mit do_issue, fertig nach dem Start """

    def __init__(self):
        super().__init__('fakeagent')
        self.calls = []

    @op("test", result={'success': 'bool'})
    def do_issue(self, name: str, task_text: str, path: str = '', wait: bool = False):
        self.calls.append((name, task_text, path, wait))
        return self.success('ok', issue={'name': name, 'status': 'fertig', 'success': True, 'branch': 'feat_' + name,
                                         'log': ['a', 'b'], 'finished': 'now'})


class FakeApi(GitlabProjectApi):
    """ GitLab-API ohne HTTP, merkt sich Label-Aenderungen und Kommentare """

    def __init__(self, owner):
        super().__init__('.', [], owner=owner)
        self.calls = []
        self.issues_data = [{'iid': 7, 'title': 'Fix Login!', 'description': 'Details'}]

    def issues_with_labels(self, labels, limit=100):
        self.calls.append(('issues', tuple(labels)))
        return self.issues_data

    def change_labels(self, iid, add='', remove=''):
        self.calls.append(('labels', iid, add, remove))

    def add_note(self, iid, body):
        self.calls.append(('note', iid, body))


class GitlabAgentTest(unittest.TestCase):

    def setUp(self):
        self.disp = LazyDispatcher()
        self.agent = FakeAgent()
        self.disp.add_runner(self.agent)
        self.owner = r.BaseJobExecutor('owner')
        self.disp.addChild(self.owner)
        self.api = FakeApi(self.owner)

    def test_owner_and_runner_discovery(self):
        self.assertIs(self.api.owner(), self.owner)
        self.assertEqual(list(self.api.agent_runners().values()), [self.agent])

    def test_do_issues_labels_and_notes(self):
        name = list(self.api.agent_runners().keys())[0]
        self.api.AGENT_POLL = 0
        started = self.api.do_issues(name)
        self.assertEqual(started, [7])
        self.assertEqual(self.agent.calls, [('7_fix_login', 'Fix Login!\n\nDetails', '.', False)])
        for _ in range(100):  # watch_issue laeuft im Thread
            if 7 not in self.api.agent_running:
                break
            import time
            time.sleep(0.02)
        self.assertIn(('labels', 7, 'Doing', 'TODO'), self.api.calls)
        self.assertEqual(self.api.calls[-1], ('labels', 7, 'Done', 'Doing'))
        notes = [c for c in self.api.calls if c[0] == 'note']
        self.assertGreaterEqual(len(notes), 2)

    def test_agent_runner_without_credentials_shows_help(self):
        runner = GitlabAgentRunner(path='.', start=False)
        self.disp.add_runner(runner)
        self.assertFalse(runner.configured())
        p = b.Page()
        runner.part_index(p)
        self.assertIn('einrichten', str(p))
        self.assertFalse(runner.gitlab_agent('do_issues')['success'])

    def test_agent_runner_with_credentials(self):
        runner = GitlabAgentRunner(token='t', project='g/r', path='.', start=False)
        self.disp.add_runner(runner)
        self.assertTrue(runner.configured())
        self.assertEqual(runner.api.base_url(), 'https://gitlab.com')
        self.assertEqual(list(runner.api.agent_runners().values()), [self.agent])
        p = b.Page()
        runner.part_index(p)
        self.assertIn('Do Issues', str(p))
        self.assertTrue(runner.agent_status()['success'])

    def test_chunks(self):
        GitlabProjectApi.NOTE_MAX_CHARS, old = 10, GitlabProjectApi.NOTE_MAX_CHARS
        try:
            self.assertEqual(GitlabProjectApi.chunks(['abc', 'def', 'ghijklmnopqrs']),
                             ['abc\ndef', 'ghijklmnop', 'qrs'])
        finally:
            GitlabProjectApi.NOTE_MAX_CHARS = old


if __name__ == '__main__':
    unittest.main()
