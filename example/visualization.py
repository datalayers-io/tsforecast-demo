import os
from typing import Dict, Tuple, TypedDict
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import timesfm
from flightsql import FlightSQLClient

matplotlib.use("Agg")

horizon = 48  # forecasting window
context = 10240  # forecasting history
qs = [0.1, 0.9]  # quantiles to show

client = FlightSQLClient(
    host="localhost",
    port=8360,
    insecure=True,
    user="admin",
    password="public",
    metadata={"db": "test", "database": "test"},
)

sql = f"select datetime, nat_demand from electricity order by datetime limit {context + horizon}"

class ForecastResult(TypedDict):
    ctx: np.ndarray
    actual: np.ndarray
    ctx_ts: np.ndarray
    future_ts: np.ndarray
    point: np.ndarray
    q_pred: Dict[float, np.ndarray]


def pull_data(
    client: FlightSQLClient,
    sql: str,
    target_col: str,
    timestamp_col: str,
) -> Tuple[np.ndarray, np.ndarray]:
    parts = []
    for ep in client.execute(sql).endpoints:
        table = client.do_get(ep.ticket).read_all()
        parts.append(
            pd.DataFrame(
                {
                    timestamp_col: pd.to_datetime(
                        np.asarray(table[timestamp_col].to_pylist(), dtype=object),
                        errors="coerce",
                    ),
                    target_col: pd.to_numeric(
                        np.asarray(table[target_col].to_pylist(), dtype=object),
                        errors="coerce",
                    ),
                }
            )
        )

    df_all = (
        pd.concat(parts, ignore_index=True)
        .dropna(subset=[timestamp_col, target_col])
        .sort_values(timestamp_col)
        .reset_index(drop=True)
    )
    series = df_all[target_col].to_numpy(dtype=np.float32)
    ts = df_all[timestamp_col].to_numpy(dtype="datetime64[ns]")
    return series, ts


def model_forecast(
    series: np.ndarray,
    ts: np.ndarray,
    context: int,
    horizon: int,
    qs: list[float],
) -> ForecastResult:
    if len(series) < context + horizon:
        raise ValueError(
            f"insufficient data: need at least {context + horizon} rows, got {len(series)}"
        )

    ctx = series[-(context + horizon) : -horizon]
    actual = series[-horizon:]
    ctx_ts = ts[-(context + horizon) : -horizon]
    future_ts = ts[-horizon:]

    model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(
        "google/timesfm-2.5-200m-pytorch"
    )
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

    _, q_raw = model.forecast(horizon=horizon, inputs=[ctx])
    q_tensor = np.asarray(q_raw)[0]
    point = np.asarray(q_tensor[:, 0]).reshape(-1)[:horizon]
    model_qs = [float(q) for q in model.model.config.quantiles]
    q_pred: Dict[float, np.ndarray] = {}
    for q in qs:
        if q not in model_qs:
            raise ValueError(f"quantile {q} not found in model quantiles: {model_qs}")
        q_pred[q] = np.asarray(q_tensor[:, 1 + model_qs.index(q)]).reshape(-1)[:horizon]

    return {
        "ctx": ctx,
        "actual": actual,
        "ctx_ts": ctx_ts,
        "future_ts": future_ts,
        "point": point,
        "q_pred": q_pred,
    }


def show_figure_and_csv(
    forecast: ForecastResult,
    output_dir: str = "outputs",
) -> None:
    ctx = forecast["ctx"]
    actual = forecast["actual"]
    ctx_ts = forecast["ctx_ts"]
    future_ts = forecast["future_ts"]
    point = forecast["point"]
    q_pred = forecast["q_pred"]

    os.makedirs(output_dir, exist_ok=True)
    csv_out = os.path.join(output_dir, "timesfm25_forecast.csv")
    fig_out = os.path.join(output_dir, "timesfm25_visualization.png")

    df = pd.DataFrame(
        {
            "datetime": future_ts,
            "step": np.arange(1, horizon + 1, dtype=np.int32),
            "actual": actual,
            "mean": point,
        }
    )
    for q in qs:
        df[f"q{int(q * 100)}"] = q_pred[q]
    df.to_csv(csv_out, index=False)

    hist = ctx[-min(128, len(ctx)) :]
    hist_ts = ctx_ts[-len(hist) :]
    x_hist = hist_ts
    x_future = np.concatenate(([hist_ts[-1]], future_ts))

    plt.figure(figsize=(10, 5))
    plt.plot(x_hist, hist, label="history(partial)", color="black", linewidth=2)
    plt.plot(
        x_future,
        np.concatenate([[hist[-1]], actual]),
        label="actual(future)",
        color="#7f7f7f",
        linestyle="--",
        linewidth=2,
    )
    plt.plot(
        x_future,
        np.concatenate([[hist[-1]], point]),
        label="TimesFM2.5 mean",
        color="#620eff",
        linewidth=2,
    )
    for q in qs:
        plt.plot(
            x_future,
            np.concatenate([[hist[-1]], q_pred[q]]),
            label=f"TimesFM2.5 q{int(q * 100)}",
            linewidth=1.8,
        )
    plt.axvline(hist_ts[-1], color="gray", linestyle="--", linewidth=1, label="forecast start")
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
    plt.savefig(fig_out, dpi=160)
    plt.close()

    print(f"Figure: {os.path.abspath(fig_out)}")
    print(f"Forecast CSV: {os.path.abspath(csv_out)}")
    print(df.head())


def main() -> None:
    series, ts = pull_data(client, sql, target_col="nat_demand", timestamp_col="datetime")
    forecast = model_forecast(series, ts, context=context, horizon=horizon, qs=qs)
    show_figure_and_csv(forecast, output_dir="outputs")


if __name__ == "__main__":
    main()
