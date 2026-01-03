import json, os
from pathlib import Path
from datetime import datetime, timedelta
import ee, pandas as pd
import geemap
from gee_utils import get_s2_collection
from indices import add_indices
from date_selector import select_dates

DATA_DIR = Path("../data")
CFG = Path("config/reservoirs.geojson")

with open(CFG, encoding="utf-8") as f:
    reservoirs = json.load(f)["features"]

for r in reservoirs:
    rid = r["properties"]["id"]
    roi = ee.Geometry(r["geometry"])

    out = DATA_DIR / rid
    (out / "images").mkdir(parents=True, exist_ok=True)

    end = datetime.utcnow().date()
    start = end - timedelta(days=365)

    col = get_s2_collection(roi, start.isoformat(), end.isoformat())
    col = col.map(add_indices)

    imgs = col.toList(col.size())
    dates = [
        datetime.fromisoformat(
            ee.Image(imgs.get(i)).date().format("YYYY-MM-dd").getInfo()
        )
        for i in range(col.size().getInfo())
    ]

    sel = select_dates(dates)

    # --- PNG保存 ---
    for k, d in sel.items():
        if k == "fallback": continue
        img = ee.Image(col.filterDate(d.isoformat(), (d+timedelta(days=1)).isoformat()).first())
        geemap.ee_export_image(
            img.select("NDCI"),
            filename=str(out / "images" / f"{k}.png"),
            region=roi,
            scale=10
        )

    # --- 時系列CSV ---
    def mean_df(img):
        d = img.date().format("YYYY-MM-dd")
        stat = img.select(["NDCI","NDTI","FAI"]).reduceRegion(
            ee.Reducer.mean(), roi, 10
        )
        return ee.Feature(None, stat).set("date", d)

    fc = col.map(mean_df).getInfo()["features"]
    rows = [{**f["properties"]} for f in fc]
    pd.DataFrame(rows).to_csv(out / "timeseries.csv", index=False)

    # --- meta.json ---
    meta = {
        "latest_date": sel["latest"].isoformat(),
        "image_dates": {k: v.isoformat() for k,v in sel.items() if k!="fallback"},
        "fallback_used": sel["fallback"],
        "updated_at": datetime.now().isoformat()
    }
    with open(out / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[OK] {rid}")
