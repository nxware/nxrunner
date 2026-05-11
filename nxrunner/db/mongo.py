
import json

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class MongoDB(r.BaseJobExecutor):
    """
     cfg mongo_url
    """
    MODULES = ['pymongo']

    def __init__(self, connection_url=None, args: u.Args = None):
        super().__init__('mongodb')
        self.ticker = None
        self.define_sig(d.PStr('op', 'insert_one'), d.PStr('db', 'main'), d.PStr('collection', 'log'))
        self.define_sig(d.PStr('op', 'find'))
        self.define_sig(d.PStr('op', 'jobs'))
        if args is None:
            args = u.Args()
        if connection_url is None:
            connection_url = args.get('mongo_url', "mongodb://localhost:27017/")
        import pymongo
        self.client = pymongo.MongoClient(connection_url)
        self.args = args
        if 'mongo_job_cron' in args:
            self.ticker = self.delayed(args.get('mongo_job_cron', 623), self.tick)

    def tick(self):
        self.execute_jobs()

    def list_database_names(self):
        return self.client.list_database_names()

    def list_collection_names(self, db):
        return self.client[db].list_collection_names()

    def find(self, db, collection_name, q={}, **kwargs):
        mdb = self.client[db]
        res = mdb.get_collection(collection_name).find(**q)
        if 'limit' in kwargs:
            res = res.limit(kwargs['limit'])
        return res

    def insert_one(self, db, collection_name, data):
        col = self.client[db][collection_name]
        x = col.insert_one(data)
        return x

    def delete_one(self, db, collection_name, q):
        col = self.client[db][collection_name]
        return col.delete_one(q)

    def part_index(self, p: b.Page, params={}):
        with p.section(h="Databases"):
            p.ul(map(lambda x: w.a(str(x), self.link(self.part_db, db=str(x))), self.list_database_names()))
        with p.section(h="nweb"):
            p(w.a("Jobs", self.link(self.part_npy)))
            p.prop("Ticker Active", self.ticker is not None)
            p(self.action_btn(dict(type=self.type, op='jobs')))
            p(" TODO  jobque verarbeiten")
            # TODO create job ui

    def create_done_writer(self):
        def process_done(data):
            self.insert_one('nweb', 'results', data)
        root = self.getRoot().jobexecutor
        # TODO or on emit
        root.add_runner(r.LambdaRunner('done', process_done))

    def execute_insert_one(self, data):
        if 'row' in data:
            row = data['row']
        elif u.contains_like(data.key, r"row\_%"):
            row = u.dict_extract_by_prefix(data, 'row_')
        else:
            row = data
        return self.success(result=str(self.insert_one(data['db'], data['collection'], row)))

    def execute_find(self, data):
        items = self.find(data['db'], data['collection'], data['q'])
        return self.success(items=items)

    def execute_create_ticker(self, data):
        self.ticker = self.delayed(self.args.get('mongo_job_cron', 623), self.tick)
        return self.success()

    def execute_jobs(self, data=dict()):
        jobs = self.find('nweb', 'jobs')
        for job in jobs:
            self.process_job(job)

    def process_job(self, job: dict):
        executor: r.BaseJobExecutor = self.getRoot().jobexecutor
        if executor.canExecute(job):
            result = executor.execute(job)
            self.delete_one('nweb', 'jobs', dict(_id=job['_id']))
            self.insert_one('nweb', 'results', result)

    def part_db(self, p: b.Page, params={}):
        dbname = params['db']
        p.h2(f"DB: {dbname}")
        p.ul(map(lambda x: w.a(x, self.link(self.part_col, db=dbname, col=x)), self.list_collection_names(dbname)))

    def part_col(self, p: b.Page, params={}):
        dbname = params['db']
        colname = params['col']
        limit = params.get('limit', 50)
        p.h2(f"DB: {dbname} Collection: {colname}")
        rows = list(self.find(dbname, colname, {}, limit=int(limit)))
        p.pre(json.dumps(rows, indent=2))

    def part_npy(self, p: b.Page, params={}):
        with p.section(h="npy"):
            p.div("Query Npy Jobs")
            if self.ticker is None:
                p(self.action_btn(dict(title="Create", type=self.type, op='create_ticker')))
            else:
                p.prop("Ticker", "active")
