from flask import Flask, render_template, request
from pathlib import Path
import json
import pandas as pd

app = Flask(__name__)

DATA_DIR = Path("../data")

RESERVOIRS = {
    "karasuhara": "烏原貯水池",
    "nunobiki": "布引貯水池"
}

def load_meta(rid):
    with open(DATA_DIR / rid / "meta.json", encoding="utf-8") as f:
        return json.load(f)

def load_timeseries(rid):
    return pd.read_csv(DATA_DIR / rid / "timeseries.csv")

@app.route("/")
def index():
    rid = request.args.get("reservoir", "karasuhara")

    meta = load_meta(rid)
    ts = load_timeseries(rid)

    charts = {
        "ndci": ts[["date", "NDCI"]].dropna().to_dict(orient="records"),
        "ndti": ts[["date", "NDTI"]].dropna().to_dict(orient="records"),
        "fai":  ts[["date", "FAI"]].dropna().to_dict(orient="records")
    }

    return render_template(
        "index.html",
        reservoirs=RESERVOIRS,
        rid=rid,
        meta=meta,
        charts=charts
    )

if __name__ == "__main__":
    app.run(debug=True)
