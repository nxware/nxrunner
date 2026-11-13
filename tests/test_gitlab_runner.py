import tempfile
import unittest
from pathlib import Path

from nwebclient import base as b
from nwebclient import runner as r
from nwebclient.runner.dispatcher import LazyDispatcher

from nxrunner.projects.dev import GitRepo
from nxrunner.projects.gitlab import Gitlab, GitlabAgentRunner


class FakeRepo(GitRepo):
    """ GitRepo ohne GitPython: nur Pfad und Remotes """

    def __init__(self, path, type, remotes):
        r.BaseJobExecutor.__init__(self, type)
        self.path = path
        self._remotes = remotes

    def remotes(self):
        return self._remotes


class GitlabTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        repos = {'repo': [{'name': 'origin', 'url': 'git@gitlab.com:grp/repo.git'}],
                 'local': [{'name': 'origin', 'url': 'https://gitlab.com/grp/local.git'}],
                 'github': [{'name': 'origin', 'url': 'git@github.com:grp/gh.git'}],
                 'mirror': [{'name': 'origin', 'url': 'git@codeberg.org:grp/m.git'},
                            {'name': 'gl', 'url': 'git@gitlab.com:grp/m.git'}]}
        self.disp = LazyDispatcher()
        for name, remotes in repos.items():
            (root / name).mkdir()
            self.disp.add_runner(FakeRepo(str(root / name), name, remotes))
        (root / 'local' / 'LOCAL.md').write_text('gitlab-token: glpat-local\n', encoding='utf-8')
        self.gitlab = Gitlab(account='@svc', token='glpat-runner')
        self.disp.add_runner(self.gitlab)

    def tearDown(self):
        self.tmp.cleanup()

    def test_projects_by_origin_domain(self):
        self.assertEqual(sorted(self.gitlab.projects().keys()), ['local', 'repo'])

    def test_api_credentials(self):
        projects = self.gitlab.projects()
        api = self.gitlab.api(projects['repo'])
        self.assertEqual((api.token(), api.params['gitlab_account']), ('glpat-runner', '@svc'))
        self.assertEqual(api.project_path(), 'grp/repo')
        self.assertEqual(api.base_url(), 'https://gitlab.com')
        self.assertEqual(self.gitlab.api(projects['local']).token(), 'glpat-local')

    def test_create_agent(self):
        res = self.gitlab.create_agent('repo')
        self.assertTrue(res['success'], res)
        agent = self.disp.get_runner('repo_gitlab_agent')
        self.assertIsInstance(agent, GitlabAgentRunner)
        self.assertFalse(agent.api.agent_active())
        self.assertEqual(agent.api.project_path(), 'grp/repo')
        self.assertEqual(agent.api.token(), 'glpat-runner')
        self.assertEqual(self.gitlab.agents_for(self.gitlab.projects()['repo'].path), ['repo_gitlab_agent'])
        self.assertFalse(self.gitlab.create_agent('repo')['success'])
        self.assertFalse(self.gitlab.create_agent('github')['success'])

    def test_part_index(self):
        p = b.Page()
        self.gitlab.part_index(p)
        html = str(p)
        self.assertIn('grp/repo', html)
        self.assertIn('create_agent', html)
        self.assertNotIn('glpat-runner', html)
        self.assertNotIn('gh.git', html)
        self.gitlab.create_agent('repo')
        p = b.Page()
        self.gitlab.part_index(p)
        self.assertIn('repo_gitlab_agent', str(p))


if __name__ == '__main__':
    unittest.main()
