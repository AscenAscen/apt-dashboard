"""국토교통부 아파트 매매·전월세 실거래가를 시군구·월 단위로 받아 JSON으로 저장합니다.
- 이미 받은 과거 월은 다시 받지 않고, 최근 3개월만 매일 갱신합니다.
- 하루 호출 한도에 닿으면 멈추고, 다음 실행 때 이어서 받습니다 (처음 며칠은 과거 데이터를 채우는 기간).
"""
import os, sys, json, time, threading, datetime as dt
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
import requests

KEY = unquote(os.environ.get("DATA_GO_KR_KEY", "").strip())
SITE = os.environ.get("SITE_DIR", "site")
START = os.environ.get("START_YM", "202101")
MAX_CALLS = int(os.environ.get("MAX_CALLS_PER_API", "8000"))   # API 하나당 하루 최대 호출 수
WORKERS = int(os.environ.get("WORKERS", "5"))
BASE = "https://apis.data.go.kr/1613000/"
EP = {"trade": "RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
      "rent": "RTMSDataSvcAptRent/getRTMSDataSvcAptRent"}
KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date().isoformat()


class Quota(Exception): pass
class Fatal(Exception): pass


def months(start, end):
    y, m = int(start[:4]), int(start[4:])
    ey, em = int(end[:4]), int(end[4:])
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y}{m:02d}")
        m += 1
        if m > 12: y, m = y + 1, 1
    return out


NOW = dt.datetime.now(KST)
CUR = f"{NOW.year}{NOW.month:02d}"
ALL = months(START, CUR)
REFRESH = set(ALL[-3:])


def num(s):
    s = (s or "").replace(",", "").strip()
    try: return int(s)
    except ValueError:
        try: return float(s)
        except ValueError: return None


def call(kind, code, ym, page, rows):
    url = BASE + EP[kind]
    params = {"serviceKey": KEY, "LAWD_CD": code, "DEAL_YMD": ym, "pageNo": page, "numOfRows": rows}
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, timeout=40)
            if r.status_code == 429: raise Quota("HTTP 429")
            root = ET.fromstring(r.content)
            if root.tag == "OpenAPI_ServiceResponse":   # 키 오류·한도 초과 등
                msg = (root.findtext(".//returnAuthMsg") or "") + " " + (root.findtext(".//returnReasonCode") or "")
                if "LIMITED" in msg or "22" == (root.findtext(".//returnReasonCode") or ""): raise Quota(msg)
                raise Fatal(msg.strip())
            rc = (root.findtext(".//resultCode") or "").strip()
            if rc in ("22", "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR"): raise Quota(rc)
            if rc not in ("00", "000", ""):
                raise Fatal(f"resultCode={rc} {root.findtext('.//resultMsg')}")
            total = int(root.findtext(".//totalCount") or 0)
            return root.findall(".//item"), total
        except (Quota, Fatal):
            raise
        except Exception as e:      # 네트워크 오류 등은 잠시 후 재시도
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"호출 실패 {kind} {code} {ym}: {last}")


def parse(kind, it):
    g = lambda t: (it.findtext(t) or "").strip()
    if kind == "trade" and g("cdealType") == "O": return None     # 해제(취소)된 거래 제외
    base = [num(g("dealDay")), g("aptNm"), g("umdNm"), num(g("excluUseAr"))]
    tail = [num(g("floor")), num(g("buildYear"))]
    if kind == "trade": return base + [num(g("dealAmount"))] + tail
    return base + [num(g("deposit")), num(g("monthlyRent"))] + tail


class Counter:
    def __init__(self): self.n, self.l, self.stop = 0, threading.Lock(), threading.Event()
    def add(self, k):
        with self.l: self.n += k


def fetch_month(kind, code, ym, cnt):
    rows, page, total = [], 1, None
    while True:
        if cnt.stop.is_set(): return None
        if cnt.n >= MAX_CALLS: cnt.stop.set(); return None
        cnt.add(1)
        items, total = call(kind, code, ym, page, 1000)
        for it in items:
            p = parse(kind, it)
            if p: rows.append(p)
        if page * 1000 >= total or not items: break
        page += 1
    return rows


def path(kind, code, ym): return os.path.join(SITE, "data", kind, code, ym + ".json")


def load_f(p):
    try:
        with open(p, encoding="utf-8") as f: return json.load(f).get("f")
    except Exception: return None


def resolve(regions):
    """강원·전북·군위처럼 코드가 바뀐 지역은 후보 중 데이터가 나오는 코드를 골라 기억해 둡니다."""
    rp = os.path.join(SITE, "data", "resolved.json")
    try:
        with open(rp, encoding="utf-8") as f: res = json.load(f)
    except Exception: res = {}
    probe_ym = ALL[-2] if len(ALL) > 1 else ALL[-1]
    for r in regions:
        if r["n"] in res: continue
        pick = r["c"][0]
        if len(r["c"]) > 1:
            for c in r["c"]:
                try:
                    _, total = call("trade", c, probe_ym, 1, 1)
                except Quota: raise
                except Exception: total = 0
                if total > 0: pick = c; break
        res[r["n"]] = pick
    os.makedirs(os.path.dirname(rp), exist_ok=True)
    with open(rp, "w", encoding="utf-8") as f: json.dump(res, f, ensure_ascii=False)
    return res


def main():
    if not KEY: sys.exit("DATA_GO_KR_KEY 가 비어 있습니다. GitHub Secrets 설정을 확인하세요.")
    with open("sigungu.json", encoding="utf-8") as f: regions = json.load(f)
    res = resolve(regions)
    codes = [(r["n"], res[r["n"]]) for r in regions]
    for kind in ("trade", "rent"):
        # 최근 3개월은 매일 갱신, 그 외에는 아직 없는 달만 받습니다. 최신 달부터 채웁니다.
        tasks = []
        for ym in reversed(ALL):
            for n, c in codes:
                p = path(kind, c, ym)
                if not os.path.exists(p) or (ym in REFRESH and load_f(p) != TODAY):
                    tasks.append((c, ym, p))
        print(f"[{kind}] 받을 항목 {len(tasks)}개, 호출 한도 {MAX_CALLS}")
        cnt = Counter()
        def work(t):
            c, ym, p = t
            try:
                rows = fetch_month(kind, c, ym, cnt)
            except Quota: cnt.stop.set(); return False
            if rows is None: return False
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "w", encoding="utf-8") as f:
                json.dump({"f": TODAY, "r": rows}, f, ensure_ascii=False, separators=(",", ":"))
            return True
        try:
            with ThreadPoolExecutor(WORKERS) as ex: done = sum(1 for ok in ex.map(work, tasks) if ok)
        except Fatal as e: sys.exit(f"API 오류로 중단: {e}")
        print(f"[{kind}] {done}개 저장, 호출 {cnt.n}회" + (" (한도 도달, 내일 이어서 받습니다)" if cnt.stop.is_set() else ""))
    build_index(regions, res)


def build_index(regions, res):
    out, pending = [], 0
    for r in regions:
        c = res[r["n"]]
        e = {"c": c, "n": r["n"]}
        for k, key in (("trade", "t"), ("rent", "r")):
            d = os.path.join(SITE, "data", k, c)
            have = sorted(x[:-5] for x in os.listdir(d)) if os.path.isdir(d) else []
            e[key] = have
            pending += len(set(ALL) - set(have))
        out.append(e)
    with open(os.path.join(SITE, "data", "index.json"), "w", encoding="utf-8") as f:
        json.dump({"updated": dt.datetime.now(KST).isoformat(timespec="minutes"), "pending": pending,
                   "total": len(ALL) * len(regions) * 2, "start": START, "regions": out},
                  f, ensure_ascii=False, separators=(",", ":"))
    print(f"색인 저장: 지역 {len(out)}곳, 남은 항목 {pending}개")


if __name__ == "__main__":
    main()
