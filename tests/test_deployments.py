"""
Unittests fuer nxrunner.projects.deployments (Hosts-Tabelle, send/add_host)

    python -m unittest tests.test_deployments
"""
import unittest
from unittest import mock

from nwebclient import base as b

from nxrunner.projects import deployments as dm
from tests.test_fs import FakeNWeb
from tests.test_jobstore import create_root


def create(nweb=True, deployment=None):
    d = dm.Deployments()
    root = create_root(d)
    root.args = {} if deployment is None else {'deployment': deployment}
    if nweb:
        root.nweb = FakeNWeb()
    return d, root


class HostTableTestCase(unittest.TestCase):

    def test_add_host_in_table(self):
        d, root = create()
        res = d.execute({'type': 'deployments', 'op': 'add_host', 'host': 'handy', 'ssh': 'pi@10.0.0.5',
                         'mobile': '1'})
        self.assertTrue(res['success'], res)
        rows = root.nweb.select("SELECT host, ssh, mobile, remote FROM deployment_hosts")
        self.assertEqual([('handy', 'pi@10.0.0.5', 1, 1)], rows)
        hosts = d.hosts()
        self.assertEqual({'host': 'handy', 'ssh': 'pi@10.0.0.5', 'mobile': True, 'remote': True}, hosts['handy'])
        # erneut senden aendert nur die Werte
        d.add_host('handy', 'pi@10.0.0.9')
        self.assertEqual('pi@10.0.0.9', d.hosts()['handy']['ssh'])
        self.assertEqual(1, len(root.nweb.select("SELECT host FROM deployment_hosts")))

    def test_hosts_merge_config_and_table(self):
        d, root = create(deployment=[{'sshhost': 'raspi5', 'path': '/home/pi/app'}])
        d.add_host('handy', 'pi@10.0.0.5')
        hosts = d.hosts()
        self.assertEqual(['raspi5', 'handy'], list(hosts))
        self.assertFalse(hosts['raspi5']['mobile'])
        self.assertTrue(hosts['handy']['mobile'])

    def test_hostinfo_in_table_not_settings(self):
        d, root = create()
        d.add_host('handy', 'pi@10.0.0.5')
        with mock.patch.object(d, 'hostinfo_cmds', return_value={'py': 'python -V'}), \
                mock.patch.object(d, 'ssh', return_value=(0, dm.Deployments.HOSTINFO_MARK + 'py\nPython 3.11')) as ssh:
            d.query_hosts(['handy'])
        self.assertEqual('pi@10.0.0.5', ssh.call_args[0][0])
        self.assertEqual('Python 3.11', d.get_hostinfo('handy')['values']['py']['value'])
        # gesendeter Eintrag bleibt erhalten
        self.assertTrue(d.hosts()['handy']['remote'])
        # offline: alte Werte bleiben
        with mock.patch.object(d, 'hostinfo_cmds', return_value={'py': 'python -V'}), \
                mock.patch.object(d, 'ssh', return_value=(255, 'timeout')):
            info = d.query_host('handy')
        self.assertFalse(info['online'])
        self.assertEqual('Python 3.11', d.get_hostinfo('handy')['values']['py']['value'])

    def test_without_persistence(self):
        d, root = create(nweb=False)
        d.add_host('handy', 'pi@10.0.0.5')
        self.assertTrue(d.hosts()['handy']['mobile'])

    def test_remove_host(self):
        d, root = create()
        d.add_host('handy')
        self.assertTrue(d.remove_host('handy')['success'])
        self.assertNotIn('handy', d.hosts())
        self.assertFalse(d.remove_host('handy')['success'])

    def test_page_hosts(self):
        d, root = create()
        d.add_host('handy', 'pi@10.0.0.5')
        p = b.Page(owner=d)
        d.part_hosts(p, {})
        html = str(p)
        self.assertIn('handy', html)
        self.assertIn('gesendet', html)
        self.assertIn('remove_host', html)


class SendTestCase(unittest.TestCase):

    def test_send_creates_mobile_entry_on_other_instance(self):
        local, _ = create()
        other, other_root = create()

        def fake_remote(url, data={}, **kwargs):
            self.assertEqual('http://server:7070/', url)
            return other.execute(dict(data))

        with mock.patch.object(dm.r, 'remote', side_effect=fake_remote), \
                mock.patch.object(dm.socket, 'gethostname', return_value='laptop'), \
                mock.patch.object(dm.getpass, 'getuser', return_value='pi'), \
                mock.patch.object(dm.nx, 'get_ip', return_value='10.0.0.5'):
            res = local.execute({'type': 'deployments', 'op': 'send', 'url': 'http://server:7070/'})
        self.assertTrue(res['success'], res)
        self.assertEqual({'host': 'laptop', 'ssh': 'pi@10.0.0.5', 'mobile': True, 'remote': True},
                         other.hosts()['laptop'])
        self.assertNotIn('laptop', local.hosts())

    def test_send_fail(self):
        d, _ = create()
        with mock.patch.object(dm.r, 'remote', return_value={'success': False, 'message': 'down'}):
            res = d.send('http://server:7070/', host='x', ssh='y')
        self.assertFalse(res['success'])
        self.assertFalse(d.send()['success'])

    def test_send_to_on_start(self):
        with mock.patch.object(dm.Deployments, 'delayed') as delayed:
            d = dm.Deployments(send_to='http://server:7070/')
        self.assertEqual(2, delayed.call_args[0][0])
        with mock.patch.object(d, 'send') as send:
            delayed.call_args[0][1]()
        send.assert_called_once_with('http://server:7070/')


if __name__ == '__main__':
    unittest.main()
