#!/usr/bin/env python3
"""Verify the provisioned plugin and every dashboard panel via Grafana's backend API."""
import base64
import json
import os
from pathlib import Path
import time
import urllib.request


def main():
    url = os.getenv("GRAFANA_URL", "http://localhost:3000")
    user = os.getenv("GRAFANA_USER", "admin")
    password = os.getenv("GRAFANA_PASSWORD", "admin")
    symbol = os.getenv("CHECK_SYMBOL", "BTCUSDT")
    headers = {
        "Authorization": "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode(),
        "Content-Type": "application/json",
    }
    health = urllib.request.Request(url + "/api/datasources/uid/dolphindb/health", headers=headers)
    with urllib.request.urlopen(health, timeout=30) as response:
        result = json.load(response)
    assert result["status"] == "OK", result
    now = int(time.time() * 1000)
    dashboard = json.loads(Path("grafana/dashboards/perps.json").read_text())
    queries = []
    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            query = dict(target)
            query.update(refId=str(panel["id"]), datasource=panel["datasource"],
                         maxDataPoints=1000, intervalMs=1000)
            query["queryText"] = query["queryText"].replace("${__from}", str(now - 300000)).replace("${__to}", str(now)).replace("$symbol", symbol)
            queries.append(query)
    request = urllib.request.Request(url + "/api/ds/query", json.dumps({
        "from": str(now - 300000), "to": str(now), "queries": queries,
    }).encode(), headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        results = json.load(response)["results"]
    for query in queries:
        result = results[query["refId"]]
        assert result.get("status") == 200 and result.get("frames") and not result.get("error"), result
    print(f"PASS: datasource health OK; all {len(queries)} panels returned Grafana data frames for {symbol}.")


if __name__ == "__main__":
    main()
