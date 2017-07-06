#!/usr/bin/env python

from faker import Factory
import csv
import subprocess
import os
import StringIO
import random
import time

fake = Factory.create()


class RunrepExecutor(object):
    _RUNREP = [os.path.join(os.environ.get("GVHOME", "/app/geneva/dev/geneva-15.2"), "bin", "runrep")]
    _FILE_RSL = "read {0};\nrunfile {0} -f csv {2} --ReportType \"{1}\";"
    _QUERY_RSL = "rungsql -f csv {1}\n{0};"

    def __init__(self, username="accessadmin", password="", agakey=5000):
        self._username = username
        self._password = password
        self._agakey = str(agakey)

    @property
    def credentials(self):
        return ["-u", self._username, "-w", self._password, "-k", self._agakey]

    def run_query(self, query, paramstr=""):
        execution = self._QUERY_RSL.format(query, paramstr)
        return self._run_runrep(execution)

    def run_raw(self, rawquery):
        return self._run_runrep(rawquery)

    def run_rsl(self, rslname, dataset="Data", paramstr=""):
        execution = self._FILE_RSL.format(rslname, dataset, paramstr)
        if rslname == "#paydowns":
            time.sleep(random.randint(10, 15))
            return _build_fake_result_paydowns()
        else:
            return self._run_runrep(execution)

    def _run_runrep(self, execution):
        runtime = self._RUNREP + self.credentials + ["-q", "-b"]
        ps = subprocess.Popen(runtime, stderr=subprocess.PIPE, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        (stdout, stderr) = ps.communicate(input=execution)
        ps.wait()
        time.sleep(1)
        return RunrepResult(stdout, stderr)


def _parse_csv_result(data):
    datastream = StringIO.StringIO(data)
    return list(csv.DictReader(datastream))


class RunrepResult(object):
    def __init__(self, rstdout, rstderr):
        self.rstdout = rstdout
        self.error = rstderr
        if rstdout is not None:
            self.data = _parse_csv_result(rstdout)
        else:
            self.data = None


_PAYDOWNS_FORMAT = [("country_code", "country"), ("currency_code", "currency"), ("date_time", "updated_at"),
                    ("pyfloat", "shares"), ("pyfloat", "nav")]


def _build_fake_result_paydowns():
    result = RunrepResult(None, None)
    rows = 99 if random.random() < 0.0675 else 100
    result.data = [{v: getattr(fake, k, None)() for k, v in _PAYDOWNS_FORMAT} for _ in xrange(rows)]
    return result
