#!/usr/bin/env python3
"""Small standard-library REST client: python3 scripts/ddb.py file.dos (or '-' for stdin)."""
import json
import os
import sys
import urllib.request


class DB:
    def __init__(self):
        self.url = os.getenv("DDB_URL", "http://localhost:8848")
        self.token = None
        login = self.post("login", {"username": os.getenv("DDB_USER", "admin"),
                                   "password": os.getenv("DDB_PASSWORD", "123456")})
        self.token = "".join(login["result"][0].split())

    def post(self, method, body):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(self.url + "/api/" + method,
                                         json.dumps(body).encode(), headers)
        with urllib.request.urlopen(request, timeout=30) as response:
            result = json.load(response)
        if str(result.get("code")) != "0":
            raise RuntimeError(result)
        return result

    def execute(self, script):
        return self.post("execute", {"script": script, "length": 10000})["result"]


if __name__ == "__main__":
    script = sys.stdin.read() if sys.argv[1] == "-" else open(sys.argv[1]).read()
    print(json.dumps(DB().execute(script), indent=2))
