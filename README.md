
# NxRunner

System Automation made simple.

## Adding Features
Creeate an EntryPoint in the category `nweb_runner` like `myname = "myns:MyRunnerClass"`, make sure your class
inherited from `nwebclient.runner.BaseJobExecutor`

```
from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d

class MyFeature(r.BaseJobExecutor):

    MODULES = ['optional_dep']

    def __init__(self):
        super().__init__('audio_convert')
        self.define_sig(d.PStr('op', 'mycall'), d.Param('infile', 'str'))

    def execute_mycall(self, data):
        return self.success()
```
You can use `part_index(self, p:b.Page, params={})` for HTML-Outputs.

## See also
 - nwebclient
 - nxml