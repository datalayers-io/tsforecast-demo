import os
import matplotlib
import numpy as np
import pandas as pd
import timesfm
from flightsql import FlightSQLClient
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 预测长度、上下文长度、需要展示的分位数
horizon = 48
context = 10240
qs = [0.1, 0.9]

client = FlightSQLClient(
    host="localhost",
    port=8360,
    insecure=True,
    user="admin",
    password="public",
    metadata={"db": "test", "database": "test"},
)

# 从 FlightSQL 拉取时间列和目标列
parts = []
for ep in client.execute(f"select datetime, nat_demand from electricity order by datetime limit {context + horizon}").endpoints:
    table = client.do_get(ep.ticket).read_all()
    parts.append(
        pd.DataFrame(
            {
                "datetime": pd.to_datetime(np.asarray(table["datetime"].to_pylist(), dtype=object), errors="coerce"),
                "nat_demand": pd.to_numeric(
                    pd.Series(np.asarray(table["nat_demand"].to_pylist(), dtype=object)),
                    errors="coerce",
                ),
            }
        )
    )
df_all = pd.concat(parts, ignore_index=True).dropna(subset=["datetime", "nat_demand"]).sort_values("datetime").reset_index(drop=True)
series = df_all["nat_demand"].to_numpy(dtype=np.float32)
ts = df_all["datetime"].reset_index(drop=True)


ctx = series[-(context + horizon) : -horizon]
actual = series[-horizon:]
ctx_ts = ts.iloc[-(context + horizon) : -horizon].reset_index(drop=True)
future_ts = ts.iloc[-horizon:].reset_index(drop=True)

model = timesfm.TimesFM_2p5_200M_torch.from_pretrained("google/timesfm-2.5-200m-pytorch")
model.compile(
    timesfm.ForecastConfig(
        max_context=context,
        max_horizon=horizon,
        normalize_inputs=True,
        use_continuous_quantile_head=True,
        force_flip_invariance=True,
        infer_is_positive=True,
        fix_quantile_crossing=True,
    )
)
point, q_raw = model.forecast(horizon=horizon, inputs=[ctx])
point = np.asarray(point)[0].reshape(-1)[:horizon]
q_tensor = np.asarray(q_raw)[0]

model_qs = [float(q) for q in model.model.config.quantiles]

# 模型分位数输出格式：
# [mean, q10, q20, ...]
q_pred = {q: np.asarray(q_tensor[:, 1 + model_qs.index(q)]).reshape(-1)[:horizon] for q in qs}

# 绘图，导出csv
csv_out = "outputs/timesfm25_forecast.csv"
os.makedirs("outputs", exist_ok=True)
df = pd.DataFrame({"step": np.arange(1, horizon + 1, dtype=np.int32), "actual": actual, "mean": point})
df.insert(0, "datetime", future_ts.to_numpy())
for q in qs:
    df[f"q{int(q * 100)}"] = q_pred[q]
df.to_csv(csv_out, index=False)

# 历史数据 + 未来真实值 + 预测分位数
hist = ctx[-min(128, len(ctx)) :]
hist_ts = ctx_ts.iloc[-len(hist):].reset_index(drop=True)
x_hist = hist_ts
x_future = pd.concat([pd.Series([hist_ts.iloc[-1]]), future_ts], ignore_index=True)

plt.figure(figsize=(10, 5))
plt.plot(x_hist, hist, label="history(partial)", color="black", linewidth=2)
plt.plot(x_future, np.concatenate([[hist[-1]], actual]), label="actual(future)", color="#7f7f7f", linestyle="--", linewidth=2)
plt.plot(x_future, np.concatenate([[hist[-1]], point]), label="TimesFM2.5 mean", color="#620eff", linewidth=2)
for q in qs:
    plt.plot(x_future, np.concatenate([[hist[-1]], q_pred[q]]), label=f"TimesFM2.5 q{int(q * 100)}", linewidth=1.8)
plt.axvline(hist_ts.iloc[-1], color="gray", linestyle="--", linewidth=1, label="forecast start")
plt.title("TimesFM2.5 forecast with selected quantiles")
plt.xlabel("datetime")
plt.ylabel("nat_demand")
plt.grid(True, alpha=0.3)
plt.legend()
ax = plt.gca()
locator = mdates.AutoDateLocator()
ax.xaxis.set_major_locator(locator)
ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
plt.tight_layout()
plt.savefig("outputs/timesfm25_visualization.png", dpi=160)
plt.close()

print(f"Figure: {os.path.abspath('outputs/timesfm25_visualization.png')}")
print(f"Forecast CSV: {os.path.abspath(csv_out)}")
print(df.head())
