
import json
import glob
import os
import base64
import time

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class MongoDB(r.BaseJobExecutor):
    """
     cfg mongo_url

     Jobs in nweb.jobs, Ergebnisse in nweb.results.
     Verschluesselte Jobs (mit CryptRunner crypt) liegen ebenfalls in nweb.jobs als {type: crypt, c: base64},
     ihre Ergebnisse verschluesselt in nweb.results als {c: base64, job_id}.

     Klartext-Felder am Job (auch bei verschluesselten Jobs):
       guid    wird ins Ergebnis uebernommen, Abfrage mit op=cresult guid=...
       target  Name der Instanz (nwebclient.nx.get_name()), die den Job ausfuehren soll; andere ignorieren ihn
       ts      Zeitpunkt des Einstellens, Jobs werden in dieser Reihenfolge ausgefuehrt

     Job-Speicher-Vertrag (z.B. fuer RemoteTerminal, andere Speicher bieten dieselben Ops an):
       op=cenqueue jobdef={...} [guid] [target]   -> result (id)
       op=cresult guid [delete=1]                 -> pending=True | pending=False, result={...}
    """
    MODULES = ['pymongo']

    def __init__(self, type='mongodb', connection_url=None, args: u.Args = None):
        super().__init__('mongodb')
        self.tick_count = 0
        self.ticker = None
        self.ticker_interval = 623
        self.define_vars('tick_count', 'ticker_interval')
        self.define_sig(d.PStr('op', 'insert_one'), d.PStr('db', 'main'), d.PStr('collection', 'log'))
        self.define_sig(d.PStr('op', 'find'), d.Param('db', 'str'), d.Param('collection', 'str'), d.Param('q', 'dict'))
        self.define_sig(d.PStr('op', 'jobs'))
        self.define_sig(d.PStr('op', 'collect_files'))
        self.define_sig(d.PStr('op', 'set_ticker'), d.PInt('value', 600))
        self.define_sig(d.PStr('op', 'stop_ticker'))
        self.define_sig(d.PStr('op', 'done_writer'), d.PStr('emitter', ''))
        self.define_sig(d.PStr('op', 'enqueue'), d.PStr('jobdef'))
        self.define_sig(d.PStr('op', 'cenqueue'), d.PStr('jobdef'), d.PStr('guid', ''), d.PStr('target', ''))
        self.define_sig(d.PStr('op', 'cresult'), d.PStr('guid'), d.PInt('delete', 1))
        self.define_sig(d.PStr('op', 'delete_doc'), d.PStr('db'), d.PStr('collection'), d.PStr('id'))
        if args is None:
            args = u.Args()
        if connection_url is None:
            connection_url = args.get('mongo_url', "mongodb://localhost:27017/")
        import pymongo
        from bson import json_util
        from nwebclient import nx
        self.json_util = json_util
        self.client = pymongo.MongoClient(connection_url)
        self.args = args
        self.instance_name = nx.get_name()
        self.result_index = False  # Index auf nweb.results.guid angelegt
        self.ticker_interval = int(args.get('mongo_job_cron', self.ticker_interval))
        if 'mongo_job_cron' in args:
            self.ticker = self.periodic(self.ticker_interval, self.tick)

    def set_ticker_interval(self, value):
        self.emit_var_change('ticker_interval', value)
        self.ticker_interval = int(value)
        if self.ticker is not None:
            self.ticker.time_s = self.ticker_interval

    def execute_set_ticker(self, data: dict):
        self.set_ticker_interval(data['value'])
        if self.ticker is None:
            self.ticker = self.periodic(self.ticker_interval, self.tick)
        return self.success(ui_reload=True)

    def tick(self):
        self.tick_count += 1
        self.execute_jobs()

    def has_crypt(self) -> bool:
        return 'crypt' in self.getRoot().jobexecutor

    def encrypt(self, value: str) -> str:
        res = self.getRoot().jobexecutor.execute(dict(type='crypt', op='encrypt', value=value))
        if not res.get('success', False):
            raise Exception(res.get('message', 'encrypt failed'))
        return res['value']

    def decrypt(self, c: str) -> str:
        res = self.getRoot().jobexecutor.execute(dict(type='crypt', op='decrypt', c=c))
        if not res.get('success', False):
            raise Exception(res.get('message', 'decrypt failed'))
        return res['value']

    def list_database_names(self):
        return self.client.list_database_names()

    def list_collection_names(self, db):
        return self.client[db].list_collection_names()

    def find(self, db, collection_name, q={}, **kwargs):
        mdb = self.client[db]
        res = mdb.get_collection(collection_name).find(q)
        if 'sort' in kwargs:
            res = res.sort(kwargs['sort'], 1)
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
            p(w.a("Crypted Jobs", self.link(self.part_npy_crypt)))
            p.prop("Ticker Active", self.ticker is not None)
            p(self.action_btn(dict(type=self.type, op='jobs', title="Fetch Jobs & Execute")))

    def create_done_writer(self, emitter: str = None):
        """
          Schreibt Ergebnisse nach nweb.results
          :param emitter: type eines Emitters, dessen on_emit-Event verwendet wird, sonst der 'done'-Runner
        """
        def process_done(data):
            self.insert_one('nweb', 'results', self.bson_safe(dict(data)))
            return self.success()
        root = self.getRoot().jobexecutor
        if emitter:
            if emitter not in root:
                return self.fail(f"Emitter {emitter} not found")
            root.get_runner(emitter).add_event_listener('on_emit', process_done)
        else:
            root.add_runner(r.LambdaRunner('done', process_done))
        return self.success()

    def execute_done_writer(self, data: dict):
        return self.create_done_writer(data.get('emitter'))

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
        q = data.get('q', {})
        if isinstance(q, str):
            q = u.load_to_dict(q)
        cursor = self.find(data['db'], data['collection'], q, limit=int(data.get('limit', 100)))
        items = [{**doc, '_id': str(doc['_id'])} if '_id' in doc else doc for doc in cursor]
        return self.success(items=self.bson_safe(items), count=len(items))

    def execute_create_ticker(self, data):
        if self.ticker is None:
            self.ticker = self.periodic(self.ticker_interval, self.tick)
        return self.success(ui_reload=True)

    def execute_stop_ticker(self, data=dict()):
        if self.ticker is not None:
            self.ticker.stop()
            self.ticker = None
        return self.success(ui_reload=True)

    def execute_jobs(self, data=dict()):
        count = 0
        for job in self.find('nweb', 'jobs', sort='ts'):
            if self.process_job(job):
                count += 1
        return self.success(count=count, ui_reload=True)

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
        """
          Verschluesselte Jobs {type: crypt, c: ...} fuehrt der CryptRunner aus, ihr Ergebnis wird verschluesselt
          gespeichert. Ohne CryptRunner bleiben sie in nweb.jobs liegen.
          Jobs mit target einer anderen Instanz bleiben ebenfalls liegen.
        """
        target = job.get('target')
        if target and target != self.instance_name:
            return False
        executor: r.BaseJobExecutor = self.getRoot().jobexecutor
        job_data = {k: v for k, v in job.items() if k not in ('_id', 'target', 'ts')}
        crypted = job_data.get('type') == 'crypt'
        if executor.canExecute(job_data):
            result = executor.execute(job_data)
            self.delete_one('nweb', 'jobs', dict(_id=job['_id']))
            if not isinstance(result, dict):
                result = dict(result=result)
            result = self.bson_safe(result)
            result.pop('_id', None)
            if crypted:
                result = dict(c=self.encrypt(json.dumps(result, default=self.json_util.default)))
            result['job_id'] = job['_id']
            if 'guid' in job:
                # im Klartext, damit der Auftraggeber sein Ergebnis ohne Entschluesseln findet
                result['guid'] = job['guid']
            self.insert_one('nweb', 'results', result)
            return True
        return False

    def bson_safe(self, value):
        """
          Werte, die BSON nicht kodieren kann, werden als Text gespeichert.
          Cursor werden als Liste der Dokumente gespeichert (max. 1000).
        """
        import datetime
        import itertools
        from bson import ObjectId
        from pymongo.cursor import Cursor
        if isinstance(value, dict):
            return {str(k): self.bson_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self.bson_safe(v) for v in value]
        if isinstance(value, Cursor):
            return [self.bson_safe(v) for v in itertools.islice(value, 1000)]
        if value is None or isinstance(value, (str, bool, int, float, bytes, datetime.datetime, ObjectId)):
            return value
        return str(value)

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

    def grid_rows(self, rows) -> list:
        """ Mongo-Dokumente fuer p.grid() serialisierbar machen """
        result = []
        for row in rows:
            item = {}
            for k, v in row.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    item[k] = v
                elif isinstance(v, (dict, list)):
                    item[k] = json.dumps(v, default=self.json_util.default)
                else:
                    item[k] = str(v)
            result.append(item)
        return result

    def grid_cols(self, rows: list) -> list:
        keys = []
        for row in rows:
            for k in row.keys():
                if k not in keys:
                    keys.append(k)
        return [w.Grid.col(k, sortable=True) for k in keys]

    def show_collection_grid(self, p: b.Page, db: str, collection: str, limit=100, map_row=None, q={}):
        """ :param map_row: fn(doc) -> doc, z.B. zum Entschluesseln """
        docs = self.find(db, collection, q, limit=limit)
        if map_row is not None:
            docs = map(map_row, docs)
        rows = self.grid_rows(docs)
        if len(rows) == 0:
            p.div("No Data")
        else:
            cols = self.grid_cols(rows)
            # tmpl wird mit str.format_map gefuellt: JSON-Klammern verdoppeln, nur {_id} bleibt Platzhalter
            job = json.dumps(dict(type=self.type, op='delete_doc', db=db, collection=collection, id='_ID_'))
            job = job.replace('{', '{{').replace('}', '}}').replace('_ID_', '{_id}')
            cols.append(w.Grid.col('_delete', title='', width=80,
                                   tmpl=f"<button onclick='exec_job({job})'>Delete</button>"))
            p.grid(rows, cols)

    def decrypt_row(self, doc: dict) -> dict:
        """ Fuer die Anzeige: c entschluesseln, bei Fehlern die Meldung anzeigen """
        res = {k: v for k, v in doc.items() if k not in ('c', 'type')}
        try:
            res['job'] = json.loads(self.decrypt(doc.get('c', '')))
        except Exception as e:
            res['error'] = str(e)
        return res

    def part_npy(self, p: b.Page, params={}):
        self.show_npy(p, crypted=False)

    def part_npy_crypt(self, p: b.Page, params={}):
        """ Wie part_npy, zeigt die verschluesselten Jobs in nweb.jobs """
        if not self.has_crypt():
            with p.section(h="npy crypt"):
                p.div("Error: CryptRunner crypt is not available. "
                      "Encrypted jobs can neither be queued nor executed.", _class='error')
            return
        self.show_npy(p, crypted=True)

    def show_npy(self, p: b.Page, crypted: bool):
        # verschluesselte und unverschluesselte Jobs liegen beide in nweb.jobs / nweb.results
        if crypted:
            jobs_q, results_q = {'type': 'crypt'}, {'c': {'$exists': True}}
        else:
            jobs_q, results_q = {'type': {'$ne': 'crypt'}}, {'c': {'$exists': False}}
        with p.section(h="npy crypt" if crypted else "npy"):
            p.div("Query Npy Jobs (nweb.jobs)")
            p(self.action_btn(dict(title="Execute Jobs now", type=self.type, op='jobs')))
            if self.ticker is None:
                p(self.action_btn(dict(title="Create Periodic Job Processor", type=self.type, op='create_ticker')))
            else:
                p.prop("Ticker", "active")
                p.prop("Interval (s)", self.ticker_interval)
                p.prop("Tick Count", self.tick_count)
                p(self.action_btn(dict(title="Stop", type=self.type, op='stop_ticker')))
        map_row = self.decrypt_row if crypted else None
        with p.section(h="Open Jobs"):
            self.show_collection_grid(p, 'nweb', 'jobs', map_row=map_row, q=jobs_q)
        with p.section(h="Results"):
            self.show_collection_grid(p, 'nweb', 'results', map_row=map_row, q=results_q)
        with p.section(h="Queue Jobs"):
            for seconds in [30, 300, 3852]:
                job = dict(type=self.type, op='set_ticker', value=seconds)
                p(self.action_btn(dict(title=f"Queue Set Ticker {seconds}s", type=self.type,
                                       op='cenqueue' if crypted else 'enqueue', jobdef=job)))
            p.ul([w.a("Queue custom Job", self.link(self.part_npy_queue, crypted=1 if crypted else 0))])
        with p.section(h="Result"):
            p.pre('', id='result')

    def part_npy_queue(self, p: b.Page, params={}):
        """
            Schreib einen Job in MongoDB, mit crypted=1 verschluesselt
        """
        crypted = str(params.get('crypted', '0')) == '1'
        if crypted and not self.has_crypt():
            p.div("Error: CryptRunner crypt is not available.", _class='error')
            return
        with p.section(h="Queue custom crypted Job" if crypted else "Queue custom Job"):
            p(self.ui_form_job('jobdef', False))
            title = "Enqueue crypted Job into MongoDB" if crypted else "Enqueue Job into MongoDB"
            op = 'cenqueue' if crypted else 'enqueue'
            p.div(self.action_btn_parametric(title, dict(type=self.type, op=op, jobdef='#jobdef')),
                  _class='ui_form_job_actions')

    def parse_jobdef(self, data: dict):
        """ :return: (job, None) oder (None, fehlermeldung) """
        job = data.get('jobdef')
        if isinstance(job, str):
            if job.strip() == '':
                return None, 'jobdef is empty'
            job = json.loads(job)
        if not isinstance(job, dict) or 'type' not in job:
            return None, 'jobdef must be a job object with type'
        return job, None

    def execute_enqueue(self, data: dict):
        """ Schreibt den Job aus jobdef (JSON) nach nweb.jobs """
        job, error = self.parse_jobdef(data)
        if error is not None:
            return self.fail(error)
        job = {**job, 'ts': time.time()}
        return self.success(result=str(self.insert_one('nweb', 'jobs', job).inserted_id))

    def execute_cenqueue(self, data: dict):
        """ Schreibt den Job aus jobdef (JSON) verschluesselt nach nweb.jobs """
        if not self.has_crypt():
            return self.fail("CryptRunner crypt not available")
        job, error = self.parse_jobdef(data)
        if error is not None:
            return self.fail(error)
        cjob = dict(type='crypt', c=self.encrypt(json.dumps(job)), ts=time.time())
        for key in ('guid', 'target'):
            if data.get(key):
                cjob[key] = str(data[key])
        return self.success(result=str(self.insert_one('nweb', 'jobs', cjob).inserted_id))

    def execute_cresult(self, data: dict):
        """
          Ergebnis eines Jobs anhand seiner guid (entschluesselt, falls verschluesselt).
          Ergebnisse anderer Jobs bleiben unberuehrt, delete=1 entfernt nur dieses Ergebnis.
        """
        guid = str(data.get('guid', '')).strip()
        if guid == '':
            return self.fail('guid fehlt')
        col = self.client['nweb']['results']
        if not self.result_index:
            col.create_index('guid')
            self.result_index = True
        doc = col.find_one({'guid': guid})
        if doc is None:
            return self.success('pending', pending=True)
        if 'c' in doc:
            try:
                result = json.loads(self.decrypt(doc['c']), object_hook=self.json_util.object_hook)
            except Exception as e:
                return self.fail(f'Ergebnis {guid} nicht entschluesselbar: {e}')
        else:
            result = {k: v for k, v in doc.items() if k not in ('_id', 'job_id', 'guid')}
        if str(data.get('delete', '1')) not in ('0', 'false', 'False'):
            col.delete_one({'_id': doc['_id']})
        return self.success('ok', pending=False, result=self.bson_safe(result))

    def execute_delete_doc(self, data: dict):
        from bson import ObjectId
        res = self.delete_one(data['db'], data['collection'], dict(_id=ObjectId(data['id'])))
        return self.success(deleted=res.deleted_count, ui_reload=True)
