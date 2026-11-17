"""
Unittests fuer nxrunner.projects.dev.Projects

    python -m unittest tests.test_projects
"""
import os
import tempfile
import unittest

from nxrunner.projects.dev import Projects


class ProjectsOpTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.a = os.path.join(self.tmp.name, 'a')
        self.b = os.path.join(self.tmp.name, 'b')
        os.makedirs(self.a)
        os.makedirs(self.b)
        with open(os.path.join(self.a, 'AGENTS.md'), 'w') as f:
            f.write('x')
        self.runner = Projects({'repos': {'a': self.a, 'b': self.b}})

    def tearDown(self):
        self.tmp.cleanup()

    def test_all(self):
        res = self.runner.execute({'op': 'projects'})
        self.assertTrue(res['success'])
        self.assertEqual([{'name': 'a', 'path': self.a}, {'name': 'b', 'path': self.b}], res['items'])

    def test_with_file(self):
        res = self.runner.execute({'op': 'projects', 'with_file': 'AGENTS.md'})
        self.assertTrue(res['success'])
        self.assertEqual([{'name': 'a', 'path': self.a}], res['items'])


if __name__ == '__main__':
    unittest.main()
