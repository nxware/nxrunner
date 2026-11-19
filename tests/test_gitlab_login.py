import unittest
from urllib.parse import parse_qs, urlsplit

from nwebclient import base as b
from nwebclient import util as u

from nxrunner.projects.gitlab_login import GitlabLogin


class FakeLogin(GitlabLogin):
    """ ohne HTTP: Token- und Benutzerabruf werden aufgezeichnet """

    def __init__(self, **kw):
        super().__init__(**kw)
        self.posted = []

    def post_token(self, data):
        self.posted.append(data)
        return {'access_token': 'tok', 'expires_in': 7200}

    def get_user(self):
        return {'username': 'alice', 'name': 'Alice', 'web_url': 'https://gl.example/alice'}


class GitlabLoginTest(unittest.TestCase):

    def test_values_from_args(self):
        args = u.Args.__new__(u.Args)
        args.get = {'gitlab.app.url': 'https://gl.example/', 'gitlab.app.app_id': 'id1',
                    'gitlab.app.secret': 's1'}.get
        login = GitlabLogin(args=args)
        self.assertEqual((login.url, login.app_id, login.secret), ('https://gl.example', 'id1', 's1'))
        self.assertTrue(login.configured())
        self.assertEqual(GitlabLogin(app_id='x', args=args).app_id, 'x')

    def test_callback_url(self):
        login = GitlabLogin(type='gl')
        self.assertEqual(login.callback_url(), 'http://localhost:7070/pysys/ui?type=gl&gl=callback')
        self.assertEqual(GitlabLogin(callback='https://h/cb').callback_url(), 'https://h/cb')

    def test_setup_form_when_missing(self):
        login = GitlabLogin()
        self.assertEqual(login.missing(), ['app_id', 'secret'])
        p = b.Page()
        login.part_index(p)
        html = str(p)
        self.assertIn('Callback-URL', html)
        self.assertIn('method="post"', html)
        self.assertIn('type="password"', html)

    def test_setup_post(self):
        login = GitlabLogin()
        p = b.Page()
        login.part_index(p, {'gl_setup': '1', 'gl_url': 'https://gl.example', 'gl_app_id': 'id',
                             'gl_secret': 'sec'})
        self.assertTrue(login.configured())
        self.assertIn('Bei GitLab anmelden', str(p))
        self.assertNotIn('sec"', str(p))

    def test_scope(self):
        args = u.Args.__new__(u.Args)
        args.get = {'gitlab.app.scope': 'openid,read_user'}.get
        login = GitlabLogin(args=args)
        self.assertEqual(login.scope, 'openid read_user')
        self.assertEqual(GitlabLogin().scope, 'read_user')
        login.configure(app_id='a', secret='s', scope='api')
        self.assertEqual(parse_qs(urlsplit(login.authorize_url()).query)['scope'], ['api'])

    def test_invalid_scope_hint(self):
        login = GitlabLogin(app_id='a', secret='s')
        p = b.Page()
        login.part_callback(p, {'error': 'invalid_scope', 'error_description': 'The requested scope is invalid'})
        self.assertIn('read_user', str(p))
        self.assertIn('anhaken', str(p))

    def test_login_flow(self):
        login = FakeLogin(url='https://gl.example', app_id='id', secret='sec', type='gl')
        query = parse_qs(urlsplit(login.authorize_url()).query)
        self.assertEqual(query['client_id'], ['id'])
        self.assertEqual(query['redirect_uri'], ['http://localhost:7070/pysys/ui?type=gl&gl=callback'])
        p = b.Page()
        login.part_callback(p, {'code': 'c1', 'state': query['state'][0]})
        self.assertIn('Angemeldet als Alice', str(p))
        self.assertEqual(login.posted[0]['redirect_uri'], query['redirect_uri'][0])
        self.assertEqual(login.access_token(), 'tok')
        # state ist verbraucht
        p = b.Page()
        login.part_callback(p, {'code': 'c1', 'state': query['state'][0]})
        self.assertIn('fehlgeschlagen', str(p))
        self.assertTrue(login.logout()['success'])
        self.assertFalse(login.logged_in())


if __name__ == '__main__':
    unittest.main()
