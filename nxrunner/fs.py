from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class Smb(r.BaseJobExecutor):

    MODULES = ['smbprotocol']

    def __init__(self):
        super(Smb, self).__init__('smb')

    def connect(self, host, username, password):
        import smbclient
        self.client = smbclient
        self.host = host
        smbclient.ClientConfig(username=username, password=password)

    def _build_path(self, path):
        path = path.replace('/', '\\')
        return "\\\\" + self.host + path

    def listdir(self, path='/'):
        return self.client.listdir(self._build_path(path))

    def get_contents(self, path="/"):
        with self.client.open_file(self._build_path(path), mode="r") as fd:
            return fd.read()

    def put_contents(self, path="/", contents=None):
        with self.client.open_file(self._build_path(path), mode="w") as fd:
            fd.write(contents)

    def copy_to_remote(self, src_path="/", dst_path="/"):
        pass
