
from nwebclient import runner as r, base
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class PiHoleApi(r.BaseJobExecutor):
    """
      https://github.com/sbarbett/pihole6api
    """
    MODULES = ['pihole6api']

    def __init__(self, url='http://127.0.0.1:7101/', password=None):
        super().__init__('pihole_api')
        from pihole6api import PiHole6Client
        self.client: PiHole6Client = PiHole6Client(url, password)

    def part_index(self, p: base.Page, params={}):
        history = self.client.metrics.get_history()
        print(history)  # {'history': [{'timestamp': 1740120900, 'total': 0, 'cached': 0 ...}]}
        queries = self.client.metrics.get_queries()
        p.div(f"Queries: {queries['recordsTotal']}")
        #p.pre(history)

        p.h2("See also")
        p.ul([w.a("pihole6api", 'https://github.com/sbarbett/pihole6api')])

