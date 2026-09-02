
from nwebclient import runner as r, base
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class Trello(r.BaseJobExecutor):
    """
    https://pypi.org/project/py-trello/
    """
    MODULES = ['py-trello']

    def __init__(self, api_key, token, args: u.Args={}):
        super().__init__('trello')
        from trello import TrelloClient
        self.api = TrelloClient(api_key=api_key,
           # api_secret='your-secret',
            token=token) #,token_secret='your-oauth-token-secret')

    def boards(self):
        return self.api.list_boards()

    def part_index(self, p: base.Page, params={}):
        pass
