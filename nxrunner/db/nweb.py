

from nwebclient import runner as r, base
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d

from nweb import DB


class DbUi(r.BaseJobExecutor):
    def __init__(self, db: DB, type='db'):
        super().__init__(type)
        self.db: DB = db

    def part_index(self, p: base.Page, params={}):
        p.input('sql', id='sql')
        # TODO exec sql

    def execute_sql(self, data):
        sql = data.get('sql')
        rows = self.db.select(sql)
        return self.success(rows=rows)