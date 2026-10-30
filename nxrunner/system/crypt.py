import base64
import binascii
import json
import os
import struct
from typing import Union

from nwebclient import runner as r
from nwebclient.runner import op
from nwebclient import base as b
from nwebclient import util as u
from nwebclient import web as w
from nwebclient import dev as d


class CryptError(Exception):
    """ Fehler mit fertiger Meldung, z.B. beschaedigte oder abgeschnittene Datei """


class CryptMixin:
    """
      Ver- und Entschluesselung mit Fernet, der Schluessel liegt in der Datei self.key_file.
    """

    def get_chipher(self):
        from cryptography import fernet
        with open(self.key_file, 'rb') as key_file:
            f = fernet.Fernet(key_file.read())
            return f

    def encrypt_file(self, infile, outfile):
        c = self.get_chipher()
        with open(infile, 'rb') as infile_stream:
            data = c.encrypt(infile_stream.read())
            with open(outfile, 'wb') as outfile_stream:
                outfile_stream.write(data)

    def decrypt_file(self, infile, outfile):
        c = self.get_chipher()
        with open(infile, 'rb') as infile_stream:
            data = c.decrypt(infile_stream.read())
            with open(outfile, 'wb') as outfile_stream:
                outfile_stream.write(data)

    def decrypt_base64(self, b64str: str) -> str:
        """
        Entschlüsselt einen Base64-kodierten String und gibt den Klartext zurück.
        """
        c = self.get_chipher()
        encrypted_data = base64.b64decode(b64str)
        decrypted_data = c.decrypt(encrypted_data)
        return decrypted_data.decode("utf-8")

    def encrypt_to_base64(self, plaintext: Union[str, bytes]) -> str:
        """
        Verschlüsselt einen String und gibt das Ergebnis Base64-kodiert zurück.
        """
        c = self.get_chipher()
        if isinstance(plaintext, str):
            plaintext_bytes = plaintext.encode("utf-8")
        else:
            plaintext_bytes = plaintext
        encrypted_data = c.encrypt(plaintext_bytes)
        return base64.b64encode(encrypted_data).decode("utf-8")

    # --- Streaming (grosse Dateien) ----------------------------------------------------------------
    # Format: MAGIC + 7 Byte Nonce-Prefix, dann Bloecke [4 Byte Laenge][AES-256-GCM Chiffretext + Tag].
    # Nonce je Block = Prefix + 4 Byte Zaehler + 1 Byte Letzter-Block-Flag (STREAM-Konstruktion),
    # dadurch fallen vertauschte, fehlende oder abgeschnittene Bloecke beim Entschluesseln auf.

    STREAM_MAGIC = b'NXC1'
    STREAM_CHUNK = 1024 * 1024
    STREAM_EXT = '.nxc'

    def get_stream_key(self) -> bytes:
        """ AES-256-Schluessel, per HKDF aus dem Fernet-Schluessel in self.key_file abgeleitet """
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        with open(self.key_file, 'rb') as key_file:
            fernet_key = base64.urlsafe_b64decode(key_file.read().strip())
        if len(fernet_key) != 32:
            raise ValueError("Fernet key must be 32 url-safe base64-encoded bytes.")
        return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b'nxrunner crypt stream').derive(fernet_key)

    @staticmethod
    def stream_nonce(prefix: bytes, counter: int, last: bool) -> bytes:
        return prefix + struct.pack('>I', counter) + (b'\x01' if last else b'\x00')

    def encrypt_stream(self, infile, outfile):
        with open(infile, 'rb') as src, open(outfile, 'wb') as dst:
            self.encrypt_stream_io(src, dst)

    def decrypt_stream(self, infile, outfile):
        with open(infile, 'rb') as src, open(outfile, 'wb') as dst:
            self.decrypt_stream_io(src, dst)

    def encrypt_stream_io(self, src, dst):
        """ src (binaer lesbar) blockweise verschluesselt nach dst schreiben """
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        aes = AESGCM(self.get_stream_key())
        prefix = os.urandom(7)
        dst.write(self.STREAM_MAGIC + prefix)
        counter = 0
        chunk = src.read(self.STREAM_CHUNK)
        while True:
            following = src.read(self.STREAM_CHUNK)
            ct = aes.encrypt(self.stream_nonce(prefix, counter, not following), chunk, None)
            dst.write(struct.pack('>I', len(ct)) + ct)
            if not following:
                break
            chunk, counter = following, counter + 1

    def decrypt_stream_io(self, src, dst):
        """ mit encrypt_stream_io verschluesselten src blockweise entschluesselt nach dst schreiben """
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.exceptions import InvalidTag
        aes = AESGCM(self.get_stream_key())

        def read_block():
            head = src.read(4)
            if not head:
                return None
            if len(head) < 4:
                raise CryptError("Decryption failed: file is truncated")
            n = struct.unpack('>I', head)[0]
            if n > self.STREAM_CHUNK + 16:
                raise CryptError("Decryption failed: invalid block length, data was modified")
            ct = src.read(n)
            if len(ct) < n:
                raise CryptError("Decryption failed: file is truncated")
            return ct

        if src.read(4) != self.STREAM_MAGIC:
            raise CryptError(f"Not a stream-encrypted file (missing {self.STREAM_MAGIC.decode()} header)")
        prefix = src.read(7)
        counter = 0
        block = read_block()
        if block is None:
            raise CryptError("Decryption failed: file is truncated")
        while block is not None:
            following = read_block()
            try:
                dst.write(aes.decrypt(self.stream_nonce(prefix, counter, following is None), block, None))
            except InvalidTag:
                raise CryptError("Decryption failed: wrong key, data was modified or truncated") from None
            block, counter = following, counter + 1


class CryptRunner(r.BaseJobExecutor, CryptMixin):
    """
      Ver- und entschlüsselt Werte und Jobs mit Fernet (Schlüsseldatei) und führt entschlüsselte Jobs auf dem
      Root-Dispatcher aus.

      type=crypt c=<base64>                          Job entschluesseln und ausfuehren
      op=encrypt value=text
      op=decrypt c=<base64>
      op=encrypt_file data=<base64> filename=x       Datei verschluesseln, value=<base64 Fernet-Token>
      op=decrypt_file data=<base64> filename=x.enc   Datei entschluesseln, value=<base64 Klartext>
      op=encrypt_job data={...}|data_*=... cjob={...} cjob_key=c
      op=encrypt_stream_file infile=x [outfile=x.nxc] [overwrite=1]   Datei auf dem Server streamend (AES-GCM)
      op=decrypt_stream_file infile=x.nxc [outfile=x] [overwrite=1]
    """

    TAGS = ['system', 'crypt']

    MODULES = ['cryptography']

    def __init__(self, key_file='rr.txt'):
        super().__init__('crypt')
        self.key_file = key_file
        # ohne op: das Ergebnis ist das des entschluesselten Jobs, daher kein fester Ergebnistyp
        self.define_sig(d.PStr('c')).set_description("Verschluesselten Job (Base64) entschluesseln und ausfuehren")
        self.define_sig(d.PStr('op', 'encrypt'), d.PStr('value')).set_description(
            "Text verschluesseln, value ist der Chiffretext (Base64)").r(success='bool', message='str', value='str')
        self.define_sig(d.PStr('op', 'decrypt'), d.PStr('c')).set_description(
            "Chiffretext (Base64) entschluesseln, value ist der Klartext").r(success='bool', message='str', value='str')
        self.define_sig(d.PStr('op', 'encrypt_file'), d.PStr('data'), d.PStr('filename', 'file')).set_description(
            "Datei (Base64) verschluesseln, value ist der Fernet-Token (Base64), filename mit .enc").r(
            success='bool', message='str', value='str', filename='str')
        self.define_sig(d.PStr('op', 'decrypt_file'), d.PStr('data'), d.PStr('filename', 'file.enc')).set_description(
            "Datei (Base64-Fernet-Token) entschluesseln, value ist der Klartext (Base64), filename ohne .enc").r(
            success='bool', message='str', value='str', filename='str')
        self.define_sig(d.PStr('op', 'encrypt_job'), d.Param('data', 'dict'), d.Param('cjob', 'dict'),
                        d.PStr('cjob_key')).set_description(
            "data verschluesseln, unter cjob_key in cjob einsetzen und cjob ausfuehren (statt data auch data_*)").r(
            success='bool', message='str', result='dict')

    def run_on_parent(self, data):
        self.info("Running on Dispatcher")
        return self.getRoot().execute(data)

    def execute(self, data):
        if 'op' in data or 'c' not in data:
            return super().execute(data)
        c, error = self.try_decrypt_base64(data['c'])
        if error is not None:
            return self.fail(error)
        try:
            res = self.run_on_parent(json.loads(c))
            return res
        except Exception as e:
            self.info(f"Exec: {c}")
            return self.fail(f"{type(e).__name__}: {e}")

    def try_decrypt_base64(self, b64str: str):
        """
        Wie decrypt_base64, wirft aber keine Exception.

        :return: (klartext, None) bei Erfolg, sonst (None, fehlermeldung)
        """
        return self.try_crypt(self.decrypt_base64, b64str, param='c')

    def try_crypt(self, func, *args, param='data'):
        """
        Fuehrt func(*args) aus und uebersetzt die typischen Fehler in eine Meldung.

        :return: (ergebnis, None) bei Erfolg, sonst (None, fehlermeldung)
        """
        from cryptography.fernet import InvalidToken
        try:
            return func(*args), None
        except CryptError as e:
            error = str(e)
        except FileNotFoundError:
            error = f"Key file not found: {self.key_file}"
        except OSError as e:
            error = f"{type(e).__name__}: {e}"
        except InvalidToken:
            error = "Decryption failed: wrong key, data was modified or not encrypted with this key"
        except UnicodeDecodeError:
            error = "Decryption failed: decrypted data is not UTF-8 text"
        except binascii.Error:
            error = f"Failed: parameter {param} is not valid base64"
        except ValueError as e:
            error = f"Invalid key in key file {self.key_file}: {e}"
        self.error(error)
        return None, error

    def encrypt_bytes_base64(self, b64str: str) -> str:
        """ Base64-Daten (z.B. Dateiinhalt) verschluesseln, Ergebnis ist der Fernet-Token Base64-kodiert """
        return base64.b64encode(self.get_chipher().encrypt(self.b64decode_data(b64str))).decode('ascii')

    def decrypt_bytes_base64(self, b64str: str) -> str:
        """ Base64-kodierten Fernet-Token entschluesseln, Ergebnis sind die Klartext-Bytes Base64-kodiert """
        return base64.b64encode(self.get_chipher().decrypt(self.b64decode_data(b64str))).decode('ascii')

    @staticmethod
    def b64decode_data(b64str: str) -> bytes:
        """ Base64 dekodieren, ein Data-URL-Prefix (data:...;base64,) wird entfernt """
        if b64str.startswith('data:') and ',' in b64str:
            b64str = b64str.split(',', 1)[1]
        return base64.b64decode(b64str, validate=True)

    def execute_encrypt(self, data):
        return self.success(value=self.encrypt_to_base64(data['value']))

    def execute_decrypt(self, data):
        c, error = self.try_decrypt_base64(data['c'])
        if error is not None:
            return self.fail(error)
        return self.success(value=c)

    def execute_encrypt_file(self, data):
        return self.crypt_file(data, self.encrypt_bytes_base64, data.get('filename', 'file') + '.enc')

    def execute_decrypt_file(self, data):
        filename = data.get('filename', 'file.enc')
        return self.crypt_file(data, self.decrypt_bytes_base64, filename[:-4] if filename.endswith('.enc') else filename)

    def crypt_file(self, data, func, filename):
        if not data.get('data'):
            return self.fail("Missing parameter: data (file content as base64)")
        value, error = self.try_crypt(func, data['data'])
        if error is not None:
            return self.fail(error)
        return self.success(value=value, filename=filename)

    @op("Datei auf dem Server streamend verschluesseln (AES-256-GCM in 1-MiB-Bloecken, ohne Base64), "
        "outfile leer = infile.nxc", result={'success': 'bool', 'message': 'str', 'outfile': 'str', 'size': 'int'})
    def encrypt_stream_file(self, infile: str, outfile: str = '', overwrite: bool = False):
        return self.stream_file(self.encrypt_stream, infile, outfile or infile + self.STREAM_EXT, overwrite)

    @op("Mit encrypt_stream_file verschluesselte Datei auf dem Server entschluesseln, "
        "outfile leer = infile ohne .nxc", result={'success': 'bool', 'message': 'str', 'outfile': 'str', 'size': 'int'})
    def decrypt_stream_file(self, infile: str, outfile: str = '', overwrite: bool = False):
        if not outfile:
            outfile = infile[:-len(self.STREAM_EXT)] if infile.endswith(self.STREAM_EXT) else infile + '.dec'
        return self.stream_file(self.decrypt_stream, infile, outfile, overwrite)

    def stream_file(self, func, infile, outfile, overwrite):
        """ func(infile, tmp) ausfuehren, erst bei Erfolg wird tmp nach outfile verschoben """
        if not os.path.isfile(infile):
            return self.fail(f"File not found: {infile}")
        if os.path.exists(outfile) and not overwrite:
            return self.fail(f"Output file exists: {outfile} (use overwrite=1)")
        tmp = outfile + '.part'
        _, error = self.try_crypt(func, infile, tmp)
        if error is not None:
            if os.path.exists(tmp):
                os.remove(tmp)
            return self.fail(error)
        os.replace(tmp, outfile)
        return self.success(outfile=outfile, size=os.path.getsize(outfile))

    def page_stream_upload(self, params={}):
        """
          POST multipart mit Feld file und mode=encrypt|decrypt, Antwort ist die ver- bzw. entschluesselte Datei.
          Das Ergebnis geht erst in eine temporaere Datei, damit beim Entschluesseln nur vollstaendig gepruefte
          Daten ausgeliefert werden; bei Fehlern Status 400 mit der Meldung als Text.
        """
        import flask
        import tempfile
        upload = params.get('file')
        if not hasattr(upload, 'stream'):
            return flask.Response("Missing upload: multipart field file", status=400, mimetype='text/plain')
        mode = params.get('mode', 'encrypt')
        if mode not in ('encrypt', 'decrypt'):
            return flask.Response("mode must be encrypt or decrypt", status=400, mimetype='text/plain')
        name = os.path.basename(upload.filename or 'file')
        if mode == 'encrypt':
            func, download_name = self.encrypt_stream_io, name + self.STREAM_EXT
        else:
            func = self.decrypt_stream_io
            download_name = name[:-len(self.STREAM_EXT)] if name.endswith(self.STREAM_EXT) else name + '.dec'
        fd, tmp = tempfile.mkstemp(prefix='nxcrypt_')
        dst = os.fdopen(fd, 'w+b')
        _, error = self.try_crypt(func, upload.stream, dst, param='file')
        if error is not None:
            dst.close()
            os.remove(tmp)
            return flask.Response(error, status=400, mimetype='text/plain')
        size = dst.tell()
        dst.seek(0)

        def send():
            try:
                while True:
                    chunk = dst.read(self.STREAM_CHUNK)
                    if not chunk:
                        break
                    yield chunk
            finally:
                dst.close()
                os.remove(tmp)

        from urllib.parse import quote
        ascii_name = download_name.encode('ascii', 'ignore').decode('ascii').replace('"', '') or 'file'
        disposition = f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(download_name)}"
        return flask.Response(send(), mimetype='application/octet-stream',
                              headers={'Content-Disposition': disposition, 'Content-Length': str(size)})

    def create_c_job(self, data):
        c = self.encrypt_to_base64(json.dumps(data))
        return dict(type=self.type, c=c)

    def execute_encrypt_job(self, data):
        if 'data' in data:
            row = data['data']
        elif u.contains_like(data.keys(), r"data\_%"):
            row = u.dict_extract_by_prefix(data, 'data_')
        else:
            return self.fail("Missing job data: use parameter 'data' or parameters with prefix 'data_'")
        for key in ('cjob', 'cjob_key'):
            if key not in data:
                return self.fail(f"Missing parameter: {key}")
        c = self.encrypt_to_base64(json.dumps(row))
        cjob = u.load_to_dict(data['cjob'])
        cjob[data['cjob_key']] = c
        result = self.run_on_parent(cjob)
        return self.success(result=result)

    def part_index(self, p: b.Page, params={}):
        p.div("Ver- und entschlüsselt Texte mit Fernet und führt verschlüsselte Jobs aus. "
              f"Schlüsseldatei: <code>{self.key_file}</code>")
        with p.section(h="Verschlüsseln (Klartext → Chiffretext)"):
            p.form_input('value', "Klartext", id='crypt_value')
            p(self.action_btn_parametric("Verschlüsseln", dict(type=self.type, op='encrypt', value='#crypt_value')))
            p.div("Ergebnis: Base64-kodierter Chiffretext")
        with p.section(h="Entschlüsseln (Chiffretext → Klartext)"):
            p.form_input('c', "Chiffretext (Base64)", id='crypt_c')
            p(self.action_btn_parametric("Entschlüsseln", dict(type=self.type, op='decrypt', c='#crypt_c')))
            p.div("Ergebnis: Klartext (UTF-8)")
        with p.section(h="Datei ver- und entschlüsseln"):
            p.form_input('crypt_file', "Datei", id='crypt_file', type='file')
            p(w.button_js("Verschlüsseln", 'crypt_file("encrypt_file");'))
            p(w.button_js("Entschlüsseln", 'crypt_file("decrypt_file");'))
            p.div("Ergebnis wird heruntergeladen: verschlüsselt als <code>&lt;name&gt;.enc</code> (Fernet-Token), "
                  "beim Entschlüsseln ohne <code>.enc</code>.")
            p.script(self.build_file_script())
        with p.section(h="Große Datei hochladen und ver- oder entschlüsseln (effizient)"):
            p.div("Direkter Upload ohne Base64, der Server verschlüsselt streamend mit AES-256-GCM in 1-MiB-Blöcken. "
                  f"Ergebnis wird heruntergeladen: <code>&lt;name&gt;{self.STREAM_EXT}</code> bzw. ohne "
                  f"<code>{self.STREAM_EXT}</code>. Nicht kompatibel mit den Fernet-<code>.enc</code>-Dateien.")
            p.form_input('crypt_upload', "Datei", id='crypt_upload', type='file')
            p(w.button_js("Verschlüsseln", 'crypt_upload("encrypt");'))
            p(w.button_js("Entschlüsseln", 'crypt_upload("decrypt");'))
            p('<div><progress id="crypt_upload_progress" max="1" value="0" style="width: 100%; display: none"></progress>'
              '<span id="crypt_upload_status"></span></div>')
            p.script(self.build_upload_script())
        with p.section(h="Datei auf dem Server ver- und entschlüsseln (effizient)"):
            p.div("Für große Dateien: AES-256-GCM in 1-MiB-Blöcken direkt auf dem Server, ohne Upload und Base64. "
                  "Der Schlüssel wird aus der Schlüsseldatei abgeleitet.")
            p.form_input('infile', "Eingabedatei (Pfad auf dem Server)", id='crypt_infile')
            p.form_input('outfile', "Ausgabedatei (leer = automatisch)", id='crypt_outfile')
            p.form_input('overwrite', "Überschreiben", id='crypt_overwrite', type='checkbox')
            stream = dict(type=self.type, infile='#crypt_infile', outfile='#crypt_outfile', overwrite='#crypt_overwrite')
            p(self.action_btn_parametric("Verschlüsseln", dict(op='encrypt_stream_file', **stream)))
            p(self.action_btn_parametric("Entschlüsseln", dict(op='decrypt_stream_file', **stream)))
            p.div(f"Verschlüsselt als <code>&lt;name&gt;{self.STREAM_EXT}</code>, beim Entschlüsseln ohne "
                  f"<code>{self.STREAM_EXT}</code>.")
        with p.section(h="Verschlüsselten Job ausführen"):
            p.div(f"Job-Aufbau: <code>{{type: {self.type}, c: &lt;Base64-Chiffretext eines Job-JSON&gt;}}</code>, "
                  "der entschlüsselte Job läuft auf dem Root-Dispatcher.")
            p.form_input('c', "Verschlüsselter Job (Base64)", id='crypt_job')
            p(self.action_btn_parametric("Entschlüsseln und ausführen", dict(type=self.type, c='#crypt_job')))

    def build_upload_script(self):
        url = '/pysys/dispatcher' + self.link(self.page_stream_upload)
        return f"""
        function crypt_upload(mode) {{
            const file = document.getElementById("crypt_upload").files[0];
            if (!file) {{ alert("Bitte eine Datei auswaehlen"); return; }}
            const bar = document.getElementById("crypt_upload_progress");
            const status = document.getElementById("crypt_upload_status");
            const show = (text, value) => {{ status.textContent = text; bar.style.display = "block"; bar.value = value; }};
            const form = new FormData();
            form.append("file", file);
            const xhr = new XMLHttpRequest();
            xhr.open("POST", "{url}&mode=" + mode);
            xhr.responseType = "blob";
            xhr.upload.onprogress = (e) => {{ if (e.lengthComputable) show("Hochladen " + Math.round(100 * e.loaded / e.total) + " %", e.loaded / e.total); }};
            xhr.upload.onload = () => show("Server " + (mode === "encrypt" ? "verschlüsselt" : "entschlüsselt") + " ...", 1);
            xhr.onprogress = (e) => {{ if (e.lengthComputable) show("Herunterladen " + Math.round(100 * e.loaded / e.total) + " %", e.loaded / e.total); }};
            xhr.onerror = () => show("Fehler: Verbindung abgebrochen", 0);
            xhr.onload = async () => {{
                if (xhr.status !== 200) {{ show("Fehler: " + await xhr.response.text(), 0); return; }}
                const cd = xhr.getResponseHeader("Content-Disposition") || "";
                const m = cd.match(/filename\\*=UTF-8''([^;]+)/) || cd.match(/filename="?([^";]+)"?/);
                const a = document.createElement("a");
                a.href = URL.createObjectURL(xhr.response);
                a.download = m ? decodeURIComponent(m[1]) : file.name;
                document.body.appendChild(a);
                a.click();
                a.remove();
                setTimeout(() => URL.revokeObjectURL(a.href), 1000);
                show("Fertig: " + a.download, 1);
            }};
            show("Hochladen 0 %", 0);
            xhr.send(form);
        }}
        """

    def build_file_script(self):
        return f"""
        function crypt_file(op) {{
            const file = document.getElementById("crypt_file").files[0];
            if (!file) {{ alert("Bitte eine Datei auswaehlen"); return; }}
            const reader = new FileReader();
            reader.onload = function() {{
                exec_job_p({{type: "{self.type}", op: op, data: reader.result, filename: file.name}}, function(r) {{
                    document.getElementById("result").innerHTML = "";
                    if (!r.success) {{ show_error(r); return; }}
                    const bin = atob(r.value);
                    const bytes = new Uint8Array(bin.length);
                    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
                    const a = document.createElement("a");
                    a.href = URL.createObjectURL(new Blob([bytes], {{type: "application/octet-stream"}}));
                    a.download = r.filename;
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                    setTimeout(() => URL.revokeObjectURL(a.href), 1000);
                }});
            }};
            reader.readAsDataURL(file);
        }}
        """
