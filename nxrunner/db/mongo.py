
import json
import glob
import os
import base64

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
        self.tick_count = 0
        self.ticker = None
        self.define_vars('tick_count')
        self.define_sig(d.PStr('op', 'insert_one'), d.PStr('db', 'main'), d.PStr('collection', 'log'))
        self.define_sig(d.PStr('op', 'find'))
        self.define_sig(d.PStr('op', 'jobs'))
        self.define_sig(d.PStr('op', 'collect_files'))
        self.define_sig(d.PStr('op', 'set_ticker'), d.PInt('value', 600))
        if args is None:
            args = u.Args()
        if connection_url is None:
            connection_url = args.get('mongo_url', "mongodb://localhost:27017/")
        import pymongo
        from bson import json_util
        self.json_util = json_util
        self.client = pymongo.MongoClient(connection_url)
        self.args = args
        if 'mongo_job_cron' in args:
            self.ticker = self.periodic(args.get('mongo_job_cron', 623), self.tick)

    def execute_set_ticker(self, data: dict):
        s = int(data['value'])
        if self.ticker is not None:
            self.ticker.time_s = s
        else:
            self.ticker = self.periodic(s, self.tick)

    def tick(self):
        self.tick_count += 1
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
        elif u.contains_like(data.keys(), r"row\_%"):
            row = u.dict_extract_by_prefix(data, 'row_')
        else:
            row = data
            row['debug_info'] = 'execute_insert_one from data'
        return self.success(result=str(self.insert_one(data['db'], data['collection'], row)))

    def execute_find(self, data):
        items = self.find(data['db'], data['collection'], u.load_to_dict(data.get('q', '{}')))
        return self.success(items=items)

    def execute_create_ticker(self, data):
        self.ticker = self.delayed(self.args.get('mongo_job_cron', 623), self.tick)
        return self.success()

    def execute_jobs(self, data=dict()):
        jobs = self.find('nweb', 'jobs')
        for job in jobs:
            self.process_job(job)

    def execute_collect_files(self, data: dict):
        db = data.get('db', 'nweb')
        collection = data.get('collection', 'files')
        count = 0
        for filepath in glob.glob(data['path']):
            if self.mv_file(filepath, db, collection, data):
                count += 1
        return self.success(count=count)

    def mv_file(self, filepath: str, db: str, collection: str, data: dict = {}) -> None:
        bin_data = u.file_get_contents(filepath)
        root = self.getRoot().jobexecutor
        crypted = 'crypt' in root
        if crypted:
            f_data = root.execute(dict(type='crypt', op='encrypt', value=bin_data))['value']
        else:
            f_data = base64.b64encode(bin_data)
        if 'force_crypt' in data and not crypted:
            return False
        mdata = dict(
            file=f_data,
            name=os.path.basename(filepath),
            crypted=crypted
        )
        self.insert_one(db, collection, mdata)
        os.remove(filepath)
        return True

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
        p.pre(json.dumps(rows, indent=2, default=self.json_util.default))

    def part_npy(self, p: b.Page, params={}):
        with p.section(h="npy"):
            p.div("Query Npy Jobs")
            if self.ticker is None:
                p(self.action_btn(dict(title="Create", type=self.type, op='create_ticker')))
            else:
                p.prop("Ticker", "active")
