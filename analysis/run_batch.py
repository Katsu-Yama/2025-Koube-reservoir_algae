import json, os
from pathlib import Path
from datetime import datetime, timedelta
import ee, pandas as pd
import geemap
from gee_utils import get_s2_collection   #gee_utils.py
from indices import add_indices   #indices.py
from date_selector import select_dates   #date_selector.py

# ------------------------------------------------------------
# このスクリプトの目的（初心者向け概要）
# ------------------------------------------------------------
# 複数の貯水池（reservoir）を順に処理して、
# - Sentinel-2 から計算した指標（NDCI など）の最新画像を PNG で保存
# - 指標の時系列を CSV として保存
# - メタ情報（最新日や使用した代替画像の情報など）を meta.json に保存
#
# 動かす前の準備（重要）:
# - Google Earth Engine (ee) のアカウントと認証が済んでいること
# - 必要なライブラリ（geemap, pandas など）がインストールされていること
# - config/reservoirs.geojson に処理対象の地物が入っていること
#
# 注: Earth Engine のオブジェクト（ee.Image, ee.ImageCollection など）はサーバー側オブジェクトです。
#      getInfo() や .get() を呼ぶとローカルにデータを取りに行きます（ブロッキング: 待ちが発生します）。
# ------------------------------------------------------------

# データ出力先ディレクトリ（ローカル）
DATA_DIR = Path("../data")
# 処理対象の GeoJSON（貯水池のポリゴン）
CFG = Path("config/reservoirs.geojson")

# GeoJSON を読み込んで features のリストを取り出す
with open(CFG, encoding="utf-8") as f:
    reservoirs = json.load(f)["features"]

# reservoirs は各貯水池を表す GeoJSON feature のリスト
for r in reservoirs:
    # 各フィーチャの properties にある id を取り出す（出力ディレクトリ名に利用）
    rid = r["properties"]["id"]

    # Earth Engine の Geometry に変換（サーバー側オブジェクト）
    roi = ee.Geometry(r["geometry"])

    # 出力先ディレクトリを作成（images サブフォルダも）
    out = DATA_DIR / rid
    (out / "images").mkdir(parents=True, exist_ok=True)

    # ここでは直近 365 日を対象にしている（UTC 日付）
    end = datetime.utcnow().date()
    start = end - timedelta(days=365)

    # gee_utils.get_s2_collection を使って Sentinel-2 のコレクションを取得
    # 引数: roi（領域）, start.isoformat(), end.isoformat()
    # get_s2_collection の中でフィルタや雲量フィルタ等をやっている想定
    col = get_s2_collection(roi, start.isoformat(), end.isoformat())

    # indices.add_indices を各画像に適用して、必要な指標（NDCI 等）を追加
    # add_indices は ee.Image を受け取り、バンド（NDCI,NDTI,FAI など）を追加して返す関数
    col = col.map(add_indices)

    # ImageCollection をリストに変換（サーバ側リスト、まだ getInfo はしていない）
    imgs = col.toList(col.size())

    # 各画像の撮影日をクライアント側の datetime に変換してリスト化
    # 注意: imgs.get(i) や .date().format().getInfo() は Earth Engine サーバーへ問い合わせを行い、
    #       getInfo() が実行されるとローカル（このスクリプト）に結果が返ってくる（ブロッキング）。
    dates = [
        datetime.fromisoformat(
            ee.Image(imgs.get(i)).date().format("YYYY-MM-dd").getInfo()
        )
        for i in range(col.size().getInfo())
    ]

    # 日付リストを date_selector.select_dates に渡して、表示や保存に使う代表日を選択する
    # select_dates は 'latest' などのキーを持つ辞書を返す（例: {"latest": dateobj, "fallback": True, ...}）
    sel = select_dates(dates)

    # --- PNG保存 ---
    # sel の中の各キー（'latest' など）に対して、その日の画像を切り出して PNG 保存する
    # ただし 'fallback' キーは真偽値などが入っている想定なのでスキップ
    for k, d in sel.items():
        if k == "fallback": continue
        # 日付 d の 0:00 から翌日 0:00 までの間の最初の画像を取得
        img = ee.Image(col.filterDate(d.isoformat(), (d+timedelta(days=1)).isoformat()).first())
        # geemap.ee_export_image でサーバー側の画像をローカルの PNG ファイルに書き出す
        # ここでは NDCI バンドを選んで出力している（scale=10 -> 10m 解像度）
        geemap.ee_export_image(
            img.select("NDCI"),
            filename=str(out / "images" / f"{k}.png"),
            region=roi,
            scale=10
        )

    # --- 時系列CSV の作成 ---
    # 各画像について、領域全体の平均値（NDCI, NDTI, FAI）を計算して時系列データを作る
    def mean_df(img):
        # img は ee.Image（サーバー側オブジェクト）
        # 日付を文字列で保持（YYYY-MM-dd 形式）
        d = img.date().format("YYYY-MM-dd")
        # reduceRegion を使って領域 roi の平均値を計算（解像度 10m）
        # 戻り値は ee.Dictionary のようなサーバー側オブジェクト
        stat = img.select(["NDCI","NDTI","FAI"]).reduceRegion(
            ee.Reducer.mean(), roi, 10
        )
        # 結果を feature として返す（クライアントに持ってくるときに扱いやすくするため）
        return ee.Feature(None, stat).set("date", d)

    # ImageCollection を map して各画像を Feature（プロパティに平均値を持つ）に変換し、
    # getInfo() で実際の辞書データとして取得する（これがブロッキングな箇所）
    fc = col.map(mean_df).getInfo()["features"]

    # fc は GeoJSON の Feature リストの形になっているので、properties 部分だけ取り出して行データにする
    rows = [{**f["properties"]} for f in fc]

    # pandas.DataFrame にして CSV に保存（index は不要）
    pd.DataFrame(rows).to_csv(out / "timeseries.csv", index=False)

    # --- meta.json の作成 ---
    # 保存したデータに関する簡単なメタ情報を作る（最新日、画像日付一覧、fallback を使ったか、更新日時）
    meta = {
        # sel['latest'] は datetime オブジェクトの想定なので isoformat() で文字列化
        "latest_date": sel["latest"].isoformat(),
        # image_dates はキー: 日付文字列 の辞書にする（fallback は含めない）
        "image_dates": {k: v.isoformat() for k,v in sel.items() if k!="fallback"},
        # fallback が使われたかどうか（True/False）
        "fallback_used": sel["fallback"],
        # ローカルで meta.json を更新した日時（iso 文字列）
        "updated_at": datetime.now().isoformat()
    }
    # meta.json を UTF-8 で書き出す（日本語対応）
    with open(out / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # 完了ログ（簡単な進捗表示）
    print(f"[OK] {rid}")
