from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class Convert(r.BaseJobExecutor):

    MODULES = ['pydub']

    def __init__(self):
        super().__init__('audio_convert')
        self.define_sig(d.PStr('op', 'tomp3'), d.Param('infile', 'str'),
                        d.Param('outfile', 'str'))

    def to_mp3(self, infile, outfile):
        self.info(f"to_mp3({infile}, {outfile})")
        import pydub
        sound = pydub.AudioSegment.from_wav(infile)
        sound.export(outfile, format="mp3")

    def execute_tomp3(self, data):
        self.to_mp3(data['infile'], data['outfile'])
        return self.success()
