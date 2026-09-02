import re
import requests
from bs4 import BeautifulSoup
import json


class PhpMyAdminClient:
    def __init__(self, base_url, username, password, database = None):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.database = database
        self.session = requests.Session()
        self.token = None

    def login(self):
        # Login-Seite laden
        r = self.session.get(self.base_url)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        token_input = soup.find("input", {"name": "token"})
        token = token_input["value"] if token_input else ""

        data = {
            "pma_username": self.username,
            "pma_password": self.password,
            "server": 1,
            "target": "index.php",
            "token": token
        }

        r = self.session.post(
            f"{self.base_url}/index.php",
            data=data,
            allow_redirects=True
        )
        r.raise_for_status()

        self.token = self._extract_token(r.text)

        if not self.token:
            raise RuntimeError("Login fehlgeschlagen oder Token nicht gefunden")

        return True

    def execute_sql(self, sql, database=None):
        if self.token is None:
            self.login()
        if database is None:
            database = self.database
        payload = {
            "db": database,
            "sql_query": sql,
            "token": self.token,
            "ajax_request": True,
            "ajax_page_request": True
        }
        r = self.session.post(
            f"{self.base_url}/index.php?route=/import",
            data=payload
        )
        r.raise_for_status()
        data = json.loads(r.text)
        return self.extract_table(data)

    def extract_table(self, response: dict) -> list[dict]:
        html = response.get("message")
        soup = BeautifulSoup(html, "html.parser")

        table = soup.find("table", class_="table_results")
        if not table:
            return []

        # Spaltennamen
        headers = [
            th.get_text(strip=True)
            for th in table.select("thead th[data-column]")
        ]

        result = []

        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")

            row = {}
            for header, cell in zip(headers, cells):
                value = cell.get_text(strip=True)

                # numerische Konvertierung
                try:
                    if "." in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    pass

                row[header] = value

            result.append(row)

        return result

    def _extract_token(self, html):
        match = re.search(r'token=([a-zA-Z0-9%]+)', html)
        if match:
            return match.group(1)

        soup = BeautifulSoup(html, "html.parser")
        token_input = soup.find("input", {"name": "token"})
        if token_input:
            return token_input.get("value")

        return None


if __name__ == "__main__":
    pma = PhpMyAdminClient(
        "https://example.com/phpmyadmin",
        "user",
        "password"
    )

    pma.login()

    result = pma.execute_sql(
        "mydatabase",
        "SELECT * FROM users LIMIT 10"
    )

    print(result)