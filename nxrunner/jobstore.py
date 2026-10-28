"""
Job-Speicher: Jobs fuer eine andere Instanz einstellen und ihre Ergebnisse per guid abholen (z.B. RemoteTerminal).

Ein Speicher ist ein Runner mit JobStoreMixin, der store_enqueue und store_result umsetzt; die Ops, ihre Parameter
und die Form der Antworten legt das Mixin fest:

    op=cenqueue jobdef={...} [guid] [target]   -> success, result (ID im Speicher)
    op=cresult guid [delete=1]                 -> success, pending=True | pending=False, result={...}

Zusagen eines Speichers:
    - Jobs werden verschluesselt abgelegt, guid und target duerfen im Klartext stehen
    - nur die Instanz target (nwebclient.nx.get_name()) fuehrt einen Job aus, ohne target jede
    - Jobs werden in der Reihenfolge des Einstellens ausgefuehrt
    - das Ergebnis ist ueber die guid des Jobs auffindbar, Ergebnisse anderer Jobs bleiben unberuehrt
"""
import json

from nwebclient import dev as d


class JobStoreMixin:

    JOB_STORE_OPS = ('cenqueue', 'cresult')

    def define_job_store_sigs(self):
        """ im __init__ des Runners aufrufen """
        self.define_sig(d.PStr('op', 'cenqueue'), d.PStr('jobdef'), d.PStr('guid', ''), d.PStr('target', ''))
        self.define_sig(d.PStr('op', 'cresult'), d.PStr('guid'), d.PInt('delete', 1))

    @classmethod
    def is_job_store(cls, runner) -> bool:
        """ Runner bietet die Ops des Vertrags an (mit oder ohne Mixin) """
        return all(callable(getattr(runner, 'execute_' + op, None)) for op in cls.JOB_STORE_OPS)

    # --- vom Speicher umzusetzen ----------------------------------------------------------------

    def store_enqueue(self, job: dict, guid: str = None, target: str = None) -> str:
        """
          Job verschluesselt ablegen, Fehler als Exception
          :return: ID im Speicher
        """
        raise NotImplementedError(type(self).__name__ + '.store_enqueue')

    def store_result(self, guid: str, delete: bool = True):
        """
          Ergebnis des Jobs mit guid (entschluesselt), Fehler als Exception
          :param delete: Ergebnis danach entfernen
          :return: Ergebnis-dict oder None, solange es noch keins gibt
        """
        raise NotImplementedError(type(self).__name__ + '.store_result')

    # --- Ops ------------------------------------------------------------------------------------

    def parse_jobdef(self, data: dict):
        """ :return: (job, None) oder (None, fehlermeldung) """
        job = data.get('jobdef')
        if isinstance(job, str):
            if job.strip() == '':
                return None, 'jobdef is empty'
            try:
                job = json.loads(job)
            except ValueError as e:
                return None, f'jobdef is no valid JSON: {e}'
        if not isinstance(job, dict) or 'type' not in job:
            return None, 'jobdef must be a job object with type'
        return job, None

    def execute_cenqueue(self, data: dict):
        """ Job aus jobdef (JSON) verschluesselt einstellen """
        job, error = self.parse_jobdef(data)
        if error is not None:
            return self.fail(error)
        try:
            job_id = self.store_enqueue(job, str(data.get('guid') or '') or None, str(data.get('target') or '') or None)
        except Exception as e:
            return self.fail(f'{type(e).__name__}: {e}')
        return self.success(result=str(job_id))

    def execute_cresult(self, data: dict):
        """ Ergebnis anhand der guid, delete=1 (Standard) entfernt nur dieses Ergebnis """
        guid = str(data.get('guid', '')).strip()
        if guid == '':
            return self.fail('guid fehlt')
        delete = str(data.get('delete', '1')) not in ('0', 'false', 'False')
        try:
            result = self.store_result(guid, delete)
        except Exception as e:
            return self.fail(f'Ergebnis {guid}: {type(e).__name__}: {e}')
        if result is None:
            return self.success('pending', pending=True)
        return self.success('ok', pending=False, result=result)
