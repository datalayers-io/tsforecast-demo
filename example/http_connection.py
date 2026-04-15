import base64
import numpy as np
import requests

headers = {"Content-Type": "application/binary"}
raw = f"admin:public".encode()
headers["Authorization"] = "Basic " + base64.b64encode(raw).decode()

params = {"db": "test"}

resp = requests.post(
    "http://localhost:8361/api/v1/sql",
    params={"db": "test"},
    data="select * from electricity order by datetime asc",
    headers=headers,
    verify=True,
)
resp.raise_for_status()
payload = resp.json()
result = payload.get("result", {})
cols = result.get("columns", [])
values = result.get("values", [])

idx = cols.index("nat_demand")
arr = np.asarray([row[idx] for row in values], dtype=object)

print("electricity.nat_demand (first 5 rows):")
print(arr[:5])
