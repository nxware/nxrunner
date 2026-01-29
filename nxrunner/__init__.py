
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
        self.define_sig(d.PStr('op', 'insert_one'), d.PStr('db', 'main'), d.PStr('collection', 'log'))
        self.define_sig(d.PStr('op', 'find'))
        self.define_sig(d.PStr('op', 'jobs'))
        if args is None:
            args = u.Args()
        if connection_url is None:
            connection_url = args.get('mongo_url', "mongodb://localhost:27017/")
        import pymongo
        self.client = pymongo.MongoClient(connection_url)

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
        p.ul(map(lambda x: w.a(str(x), self.link(self.part_db, db=str(x))), self.list_database_names()))

    def execute_insert_one(self, data):
        if 'row' in data:
            row = data['row']
        else:
            row = data
        return self.success(result=str(self.insert_one(data['db'], data['collection'], row)))

    def execute_find(self, data):
        items = self.find(data['db'], data['collection'], data['q'])
        return self.success(items=items)

    def execute_jobs(self, data):
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
        p.h2(f"DB: {dbname} Collection: {colname}")
        p.pre(json.dumps(list(self.find(dbname, colname, {}, limit=50)), indent=2))

