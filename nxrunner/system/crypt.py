import base64
import binascii
import json
from typing import Union

from nwebclient import runner as r
from nwebclient import base as b
from nwebclient import util as u


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


class CryptRunner(r.BaseJobExecutor, CryptMixin):
    """
      Ver- und entschlüsselt Werte und Jobs mit Fernet (Schlüsseldatei) und führt entschlüsselte Jobs auf dem
      Root-Dispatcher aus.

      type=crypt c=<base64>                          Job entschluesseln und ausfuehren
      op=encrypt value=text
      op=decrypt c=<base64>
      op=encrypt_job data={...}|data_*=... cjob={...} cjob_key=c
    """

    TAGS = ['system', 'crypt']

    MODULES = ['cryptography']

    def __init__(self, key_file='rr.txt'):
        super().__init__('crypt')
        self.key_file = key_file

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
        from cryptography.fernet import InvalidToken
        try:
            return self.decrypt_base64(b64str), None
        except FileNotFoundError:
            error = f"Key file not found: {self.key_file}"
        except InvalidToken:
            error = "Decryption failed: wrong key, data was modified or not encrypted with this key"
        except UnicodeDecodeError:
            error = "Decryption failed: decrypted data is not UTF-8 text"
        except binascii.Error:
            error = "Decryption failed: parameter c is not valid base64"
        except ValueError as e:
            error = f"Invalid key in key file {self.key_file}: {e}"
        self.error(error)
        return None, error

    def execute_encrypt(self, data):
        return self.success(value=self.encrypt_to_base64(data['value']))

    def execute_decrypt(self, data):
        c, error = self.try_decrypt_base64(data['c'])
        if error is not None:
            return self.fail(error)
        return self.success(value=c)

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
        p("Führt verschlüsselte Jobs aus.")
        p("Struktur {c:base64_encoded_crypt_json}")
        # TODO Form vor createtio
        with p.section(h="Decrypt"):
            p.form_input('c', "Base64 encodes", id='c')
            p(self.action_btn_parametric("Decrypt", dict(type=self.type, op='decrypt', c='#c')))
