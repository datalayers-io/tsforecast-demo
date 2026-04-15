import numpy as np
from flightsql import FlightSQLClient

client = FlightSQLClient(
    host="localhost",
    port=8360,
    insecure=True,
    user="admin",
    password="public",
    metadata={"db": "test", "database": "test"},
)

info = client.execute("select * from electricity")
parts = []

for endpoint in info.endpoints:
    reader = client.do_get(endpoint.ticket)
    table = reader.read_all()
    parts.append(np.asarray(table["nat_demand"].to_pylist(), dtype=object))

if parts:
    arr = np.concatenate(parts)
else:
    arr = np.asarray([], dtype=object)

print("electricity.nat_demand (first 5 rows):")
print(arr[:5])
