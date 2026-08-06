import os
import re
import sys
import configparser as _cp
import pandas as pd
import io
from pathlib import Path

_APP_DIR = Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent
_cfg = _cp.ConfigParser(interpolation=None)  # ปิด interpolation ให้ใช้ %USERPROFILE% ใน path ได้
_cfg.read(_APP_DIR / "config.ini", encoding="utf-8")
_CALENDAR_LOCAL_PATH = os.path.expandvars(_cfg.get("paths", "calendar", fallback=""))
from urllib.request import Request, urlopen
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse


def _with_download_flag(url: str) -> str:
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q["download"] = "1"
    return urlunparse(parsed._replace(query=urlencode(q)))


def _looks_like_html(data: bytes) -> bool:
    head = data[:1024].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")


def _looks_like_excel(data: bytes) -> bool:
    return data.startswith(b"PK") or data.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1")


def _pick_date_col(df: pd.DataFrame):
    exact = next((c for c in df.columns if str(c).strip().upper() == "DATE"), None)
    if exact is not None:
        return exact

    best_col = None
    best_score = 0.0
    for c in df.columns:
        s = pd.to_datetime(df[c], errors="coerce")
        score = float(s.notna().mean()) if len(s) else 0.0
        if score > best_score:
            best_score = score
            best_col = c
    if best_col is None or best_score < 0.2:
        raise RuntimeError("Cannot infer DATE column from calendar source")
    return best_col


def _map_status_series(raw: pd.Series) -> pd.Series:
    num = pd.to_numeric(raw, errors="coerce")
    if num.notna().mean() >= 0.5:
        out = num.round().astype("Int64")
        return out.where(out.isin([0, 1]))

    txt = raw.astype(str).str.strip().str.lower()
    mapped = txt.map(
        {
            "1": 1,
            "0": 0,
            "working": 1,
            "work": 1,
            "holiday": 0,
            "1 ทำงาน": 1,
            "0 วันหยุด": 0,
            "ทำงาน": 1,
            "วันหยุด": 0,
        }
    )
    return mapped.astype("Int64")


def _pick_status_col(df: pd.DataFrame):
    exact = next((c for c in df.columns if str(c).strip().lower() == "status"), None)
    if exact is not None:
        return exact

    best_col = None
    best_score = 0.0
    for c in df.columns:
        mapped = _map_status_series(df[c])
        score = float(mapped.notna().mean()) if len(mapped) else 0.0
        if score > best_score:
            best_score = score
            best_col = c
    if best_col is None or best_score < 0.5:
        raise RuntimeError("Cannot infer status column from calendar source")
    return best_col


def _read_calendar_any(source: Path | str, sheet_name: str) -> pd.DataFrame:
    if isinstance(source, Path):
        data = source.read_bytes()
    elif isinstance(source, str) and source.lower().startswith(("http://", "https://")):
        req = Request(source, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=60) as resp:
            data = resp.read()
    else:
        data = Path(source).read_bytes()

    try:
        return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name)
    except Exception:
        pass

    # sheet name ไม่ตรง → ลอง sheet แรกที่มีอยู่จริง
    if _looks_like_excel(data):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
            first_sheet = wb.sheetnames[0]
            wb.close()
            return pd.read_excel(io.BytesIO(data), sheet_name=first_sheet)
        except Exception:
            pass

    for enc in ("utf-8-sig", "utf-8", "cp874", "latin1"):
        try:
            text = data.decode(enc)
            df = pd.read_csv(io.StringIO(text))
            if not df.empty:
                return df
        except Exception:
            continue

    for enc in ("utf-8", "cp874", "latin1"):
        try:
            text = data.decode(enc, errors="ignore")
            tables = pd.read_html(io.StringIO(text))
            if tables:
                return tables[0]
        except Exception:
            continue

    raise RuntimeError("Cannot parse calendar source as Excel/CSV/HTML")


def _get_sharepoint_token(sharepoint_host: str) -> str | None:
    """
    ขอ access token จาก Microsoft ด้วย Device Code Flow (รองรับ MFA)
    - ครั้งแรก: แสดง URL + code ให้ login ผ่านเบราว์เซอร์ แล้ว cache token ไว้
    - ครั้งต่อไป: ใช้ cached token อัตโนมัติ (ไม่ต้อง login ซ้ำ)
    """
    try:
        import msal
        import json

        TENANT_ID = "5eee0a79-8e04-4820-afe9-907a209c04eb"
        CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"  # Microsoft Office public client
        SCOPES = [f"{sharepoint_host}/AllSites.Read"]
        CACHE_FILE = Path(__file__).parent / "sp_token_cache.bin"

        cache = msal.SerializableTokenCache()
        if CACHE_FILE.exists():
            cache.deserialize(CACHE_FILE.read_text(encoding="utf-8"))

        app = msal.PublicClientApplication(
            CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{TENANT_ID}",
            token_cache=cache,
        )

        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(SCOPES, account=accounts[0])
            if result and "access_token" in result:
                if cache.has_state_changed:
                    CACHE_FILE.write_text(cache.serialize(), encoding="utf-8")
                return result["access_token"]

        flow = app.initiate_device_flow(scopes=SCOPES)
        if "user_code" not in flow:
            print(f"⚠️ MSAL device flow error: {flow}")
            return None

        print("\n" + "=" * 60)
        print("📋 กรุณา login เพื่อดาวน์โหลด Calendar จาก SharePoint:")
        print(f"   1. เปิดเบราว์เซอร์ไปที่: {flow['verification_uri']}")
        print(f"   2. พิมพ์ code: {flow['user_code']}")
        print(f"   3. Login ด้วย wicharit.l@nanyangtextile.com (รวม MFA)")
        print("=" * 60 + "\n")

        result = app.acquire_token_by_device_flow(flow)

        if "access_token" in result:
            if cache.has_state_changed:
                CACHE_FILE.write_text(cache.serialize(), encoding="utf-8")
            print("✅ Login สำเร็จ token ถูก cache ไว้แล้ว (ครั้งต่อไปไม่ต้อง login ซ้ำ)")
            return result["access_token"]

        print(f"⚠️ MSAL auth error: {result.get('error_description', result)}")
    except Exception as e:
        print(f"⚠️ MSAL ไม่สามารถใช้งานได้: {e}")
    return None


def _download_calendar_file(file_url: str, local_path: Path) -> Path:
    parsed = urlparse(file_url)
    sharepoint_host = f"{parsed.scheme}://{parsed.netloc}"

    token = _get_sharepoint_token(sharepoint_host)

    candidates = [_with_download_flag(file_url), file_url]
    last_error = None

    for url in candidates:
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            req = Request(url, headers=headers)
            with urlopen(req, timeout=60) as resp:
                data = resp.read()

            if _looks_like_excel(data):
                local_path.write_bytes(data)
                print(f"✅ ดาวน์โหลด Calendar.xlsx สำเร็จ → {local_path}")
                return local_path

            last_error = RuntimeError("Response is not a valid Excel file")
        except Exception as e:
            last_error = e

    if local_path.exists():
        print(
            f"⚠️ ดาวน์โหลด Calendar จาก SharePoint ไม่สำเร็จ ({last_error}) — ใช้ไฟล์ local ล่าสุดแทน: {local_path}"
        )
        return local_path

    raise RuntimeError(
        f"Cannot download Calendar.xlsx. Last error: {last_error}\n"
        f"กรุณาตั้ง environment variables SP_USERNAME และ SP_PASSWORD ด้วยอีเมล์/รหัสผ่านองค์กร"
    )


def load_calendar(file_path: Path | str, sheet_name: str = "Work day") -> pd.DataFrame:
    """
    Calendar master (Daily level)

    status = 1 : working day
    status = 0 : holiday (ห้ามวางแผน)
    """

    # ใช้ไฟล์ local เท่านั้น ไม่ใช้ SharePoint
    source = _CALENDAR_LOCAL_PATH
    if not source:
        raise FileNotFoundError(
            f"ไม่ได้ตั้งค่า path ของ Calendar.xlsx\n"
            f"กรุณาตั้งค่าใน config.ini หัวข้อ [paths] calendar (ที่ {_APP_DIR / 'config.ini'})"
        )

    try:
        df = _read_calendar_any(source, sheet_name=sheet_name)
    except Exception as e:
        raise FileNotFoundError(
            f"อ่าน Calendar ไม่ได้จาก {source} ({e})\n"
            f"กรุณาแก้ path ใน config.ini หัวข้อ [paths] calendar ให้ตรงกับเครื่องของคุณ"
        ) from e

    # clean column names
    df.columns = df.columns.str.strip()

    try:
        date_col = _pick_date_col(df)
        status_col = _pick_status_col(df)
    except Exception as e:
        raise ValueError(
            f"ไฟล์ Calendar ({source}) ไม่มีคอลัมน์ DATE/status ที่อ่านได้ ({e})\n"
            f"กรุณาตรวจสอบรูปแบบไฟล์ Calendar.xlsx"
        ) from e

    # standard datetime
    df["DATE"] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df["DATE"].notna()].copy()

    # map working day flag
    # 1 = working day, 0 = holiday
    df["status"] = _map_status_series(df[status_col])
    df = df[df["status"].notna()].copy()
    df["status"] = df["status"].astype(int)
    df["is_working_day"] = df["status"].map({1: 1, 0: 0})

    # sort for safety
    df = df.sort_values("DATE").reset_index(drop=True)

    # ใช้ WEEK/YEAR จาก source file ถ้ามี (บริษัทกำหนดเองเช่น week 17 ครอบสงกรานต์)
    # ถ้าไม่มี → fallback คำนวณด้วย ISO+3 shift
    _src_week_col = next((c for c in df.columns if str(c).strip().upper() == "WEEK"), None)
    _src_year_col = next((c for c in df.columns if str(c).strip().upper() == "YEAR"), None)
    if _src_week_col and pd.to_numeric(df[_src_week_col], errors="coerce").notna().mean() > 0.8:
        df["WEEK"] = pd.to_numeric(df[_src_week_col], errors="coerce").fillna(0).astype(int)
        if _src_year_col:
            df["YEAR"] = pd.to_numeric(df[_src_year_col], errors="coerce").fillna(df["DATE"].dt.year).astype(int)
        else:
            df["YEAR"] = df["DATE"].dt.year
    else:
        # fallback: Friday–Thursday (บวก 3 วัน → ISO week)
        _shifted = df["DATE"] + pd.Timedelta(days=3)
        iso = _shifted.dt.isocalendar()
        df["YEAR"] = iso["year"].astype(int)
        df["WEEK"] = iso["week"].astype(int)
    # ── NORMALIZE ปีของ "สัปดาห์เศษปลายปี" ────────────────────────────────
    # ปกติสัปดาห์ที่คร่อมปีจะถูก label เป็น "ปีถัดไป W1" ซึ่งถูกต้อง
    # (เช่น 2027-12-31..2028-01-06 = 2028 W1)
    # แต่บางปีมีเศษวันปลายปีที่ไม่พอเป็นสัปดาห์เต็ม แล้วถูก label เป็น
    # "ปีถัดไป W53" ทั้งที่วันทั้งหมดอยู่ในปีก่อน
    # (เช่น 2026-12-28..2026-12-31 ถูก label 2027 W53 — ต้องเป็น 2026 W53)
    # ผลเสียถ้าไม่แก้: YW_INT = 202753 → order ที่กรอก FG 202653 หาไม่เจอ
    # และ FG 202753 (ตั้งใจหมายถึงปลายปี 2027) จะชี้ย้อนไป ธ.ค. 2026 ผิด 1 ปีเต็ม
    # เงื่อนไข: WEEK >= 52 (เศษปลายปี ไม่ใช่ W1 ของปีใหม่) และ YEAR ล้ำหน้าปีจริงของวันที่
    _wk_year = df.groupby(["YEAR", "WEEK"])["DATE"].transform("min").dt.year
    _mislabeled = (df["WEEK"] >= 52) & (df["YEAR"] > _wk_year)
    if _mislabeled.any():
        _fix = df.loc[_mislabeled, ["YEAR", "WEEK"]].drop_duplicates()
        for _, _r in _fix.iterrows():
            _m = _mislabeled & (df["YEAR"] == _r["YEAR"]) & (df["WEEK"] == _r["WEEK"])
            _true_year = int(df.loc[_m, "DATE"].min().year)
            print(
                f"[CALENDAR FIX] สัปดาห์เศษปลายปี {int(_r['YEAR'])} W{int(_r['WEEK'])} "
                f"({df.loc[_m, 'DATE'].min().date()}..{df.loc[_m, 'DATE'].max().date()}) "
                # ใช้ ASCII arrow เท่านั้น — Calendar.py ถูก import โดย AVA_MC.py
                # ซึ่งไม่ได้ reconfigure stdout เป็น utf-8 (console เป็น cp874)
                # อักขระอย่าง U+2192 จะทำให้ UnicodeEncodeError ทั้ง pipeline
                f"-> แก้ YEAR เป็น {_true_year}"
            )
        df.loc[_mislabeled, "YEAR"] = df.loc[_mislabeled, "DATE"].dt.year

    df["MONTH"] = df["DATE"].dt.month
    # Year-Week key (e.g., 2026-02) for grouping
    df["YW"] = df["YEAR"].astype(str) + "-" + df["WEEK"].astype(str).str.zfill(2)
    # Short day name used in plan (Mon, Tue, ...)
    df["DAY"] = df["DATE"].dt.day_name().str[:3]

    return df


# =========================================================
# WORK DAY — วันทำงานราย (Factory, MC_CAT, Guage) [+ ราย WEEK]
# แหล่งเดียวของความจริง: ชีท "Work Day" ใน Calendar.xlsx
#   Factory | MC_CAT | Guage | WEEK | WORK_DAY
#   WEEK ว่าง  = ค่ามาตรฐานของกลุ่ม (ใช้ทุกสัปดาห์)
#   WEEK มีเลข = ค่าเฉพาะสัปดาห์นั้น
# ค่าที่กรอกใช้ตรง ๆ ไม่หักวันหยุดซ้ำ · ไม่มีในชีท = ไม่วางแผน (0 วัน)
# =========================================================
WORKDAY_SHEET = "Work Day"
WEEK_MERGE_SHEET = "Week Merge"
DEFAULT_WORK_DAYS = 6.0    # หากลุ่มในชีท Work Day ไม่เจอ → ใช้ 6 วัน (ยังหักวันหยุดตามปฏิทินอยู่)
NORMAL_CAL_DAYS = 6.0      # สัปดาห์ปกติ (ศุกร์–พฤหัส) เปิดกี่วัน — ใช้วัด "วันหยุดส่วนเกิน" ของสัปดาห์นั้น
DEFAULT_WORK_HOURS = 24.0  # ไม่ตั้ง WORK_HOUR = 24 ชม. (ไม่ลดกำลังผลิต)


def _norm_key(v) -> str:
    s = str(v).strip().upper()
    if s in ("NAN", "NONE"):
        return ""
    if re.fullmatch(r"\d+\.0+", s):  # 12.0 → 12 (Guage อ่านจาก Excel เป็น float)
        return s.split(".", 1)[0]
    return s


def _num(v):
    try:
        f = float(str(v).strip())
    except (TypeError, ValueError):
        return None
    return None if pd.isna(f) else f


def load_workday(file_path: Path | str | None = None) -> dict:
    """อ่านชีท Work Day → {"default": {(fac,cat,gauge): วัน}, "week": {(fac,cat,gauge,week): วัน},
    "hour": {(fac,cat,gauge): ชั่วโมง}}
    ไม่มีชีท/อ่านไม่ได้ → คืน dict ว่าง (ทุกกลุ่มจะใช้ DEFAULT_WORK_DAYS / ชั่วโมงตามเดิม)"""
    src = file_path or _CALENDAR_LOCAL_PATH
    out = {"default": {}, "week": {}, "hour": {}}
    try:
        df = pd.read_excel(src, sheet_name=WORKDAY_SHEET)
    except Exception:
        return out

    df.columns = [str(c).strip() for c in df.columns]
    cols = {str(c).strip().upper(): c for c in df.columns}
    need = ("FACTORY", "MC_CAT", "GUAGE", "WORK_DAY")
    if any(k not in cols for k in need):
        return out

    has_hour = "WORK_HOUR" in cols
    for _, r in df.iterrows():
        key = (_norm_key(r[cols["FACTORY"]]), _norm_key(r[cols["MC_CAT"]]), _norm_key(r[cols["GUAGE"]]))
        if not any(key):
            continue
        days = _num(r[cols["WORK_DAY"]])
        wk = _num(r[cols["WEEK"]]) if "WEEK" in cols else None
        if wk is None:
            # ค่ามาตรฐานของกลุ่ม: วัน + ชั่วโมง (ชั่วโมงเป็น override ต่อกลุ่มอย่างเดียว ไม่รายสัปดาห์)
            if days is not None:
                out["default"][key] = days
            if has_hour:
                hr = _num(r[cols["WORK_HOUR"]])
                if hr is not None:
                    out["hour"][key] = hr
        elif days is not None:
            out["week"][key + (int(wk),)] = days
    return out


def load_week_merge(file_path: Path | str | None = None) -> dict:
    """อ่านชีท Week Merge (ยุบสัปดาห์ ใช้ทั้งระบบ) → {week: target_week} หลังคลาย chain แล้ว
    เช่น W31 ยุบเข้า W32 → {31: 32} : งานไม่ลง W31, วันทำงาน W32 = W31 + W32"""
    src = file_path or _CALENDAR_LOCAL_PATH
    try:
        df = pd.read_excel(src, sheet_name=WEEK_MERGE_SHEET)
    except Exception:
        return {}

    cols = {str(c).strip().upper(): c for c in df.columns}
    if "WEEK" not in cols or "MERGE_TO" not in cols:
        return {}

    raw = {}
    for _, r in df.iterrows():
        src_w, dst_w = _num(r[cols["WEEK"]]), _num(r[cols["MERGE_TO"]])
        if src_w is None or dst_w is None or int(src_w) == int(dst_w):
            continue
        raw[int(src_w)] = int(dst_w)

    # คลาย chain (31→32, 32→33 ⇒ 31→33) และตัดวงจร
    out = {}
    for w in raw:
        seen, cur = {w}, raw[w]
        while cur in raw and cur not in seen:
            seen.add(cur)
            cur = raw[cur]
        out[w] = cur
    return out


def calendar_week_map(calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    Weekly calendar summary
    ใช้สำหรับ logic ระดับสัปดาห์
    """

    weekly = (
        calendar_df
        .groupby("YW", as_index=False)
        .agg(
            year=("YEAR", "first"),
            month=("MONTH", "first"),
            week=("WEEK", "first"),
            week_start=("DATE", "min"),
            week_end=("DATE", "max"),
            working_days=("is_working_day", "sum"),
            total_days=("DATE", "count")
        )
        # เรียงตามวันที่จริง (week_start) ไม่ใช่ [year, week]
        # กันกรณีสัปดาห์คร่อมปีที่ถูก label ปีถัดไป (เช่น 28–31 ธ.ค. label เป็น YEAR ปีหน้า WEEK 53)
        # ให้ timeline เรียงตามลำดับเวลาจริงเสมอ
        .sort_values("week_start")
        .reset_index(drop=True)
    )

    return weekly


if __name__ == "__main__":
    # =========================
    # Run only for validation
    # =========================
    cal_df = load_calendar(_CALENDAR_LOCAL_PATH, sheet_name="Sheet1")
    cal_week_df = calendar_week_map(cal_df)

    # สรุปช่วงครอบคลุม — ให้เห็นชัดว่าปฏิทินมีถึงปีไหน (ยืนยันว่า auto-extend ทำงาน)
    _years = sorted(cal_df["YEAR"].dropna().astype(int).unique().tolist())
    print("=== CALENDAR COVERAGE ===")
    print(f"   ช่วงวันที่ : {cal_df['DATE'].min().date()}  ->  {cal_df['DATE'].max().date()}")
    print(f"   ปีที่มี    : {_years}")
    print(f"   จำนวนสัปดาห์: {len(cal_week_df)}  |  จำนวนวัน: {len(cal_df)}")

    print("\n=== DAILY CALENDAR (head) ===")
    print(cal_df.head(10))

    print("\n=== DAILY CALENDAR (tail) ===")
    print(cal_df.tail(5))

    print("\n=== WEEKLY CALENDAR (head) ===")
    print(cal_week_df.head())
