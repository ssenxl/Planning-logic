import configparser as _cp
import pandas as pd
import os
from pathlib import Path

_APP_DIR = Path(__file__).parent
_cfg = _cp.ConfigParser(interpolation=None)  # ปิด interpolation ให้ใช้ %USERPROFILE% ใน path ได้
_cfg.read(_APP_DIR / "config.ini", encoding="utf-8")
from datetime import datetime, timedelta


def get_current_week():
    """
    หา week ปัจจุบันตาม Calendar logic
    Week definition: Friday–Thursday (เริ่มวันศุกร์ จบวันพฤหัสบดี)
    """
    today = datetime.now()
    # บวก 3 วัน เพื่อให้ศุกร์กลายเป็นจันทร์ แล้วใช้ ISO week
    shifted = today + timedelta(days=3)
    year = shifted.isocalendar()[0]
    week = shifted.isocalendar()[1]
    return year, week


def add_stock_min_from_target(df_stock, target_file=None):
    """
    เพิ่ม column STOCK_MIN จากไฟล์ Target Stock
    
    Args:
        df_stock: DataFrame ที่มี ITEM_CODE
        target_file: path ไปยังไฟล์ Target Stock
    
    Returns:
        DataFrame with STOCK_MIN column
    """
    if target_file is None:
        target_file = str(_APP_DIR / "Estimate_Core" / "Target_Stock.xlsx")
    
    if not os.path.exists(target_file):
        print(f"⚠️  ไม่พบไฟล์ Target Stock: {target_file}")
        df_stock['STOCK_MIN'] = 0
        return df_stock
    
    try:
        # อ่านไฟล์ Target Stock
        df_target = pd.read_excel(target_file)
        print(f"\n✓ อ่านไฟล์ Target Stock: {len(df_target)} แถว")
        print(f"  Columns: {df_target.columns.tolist()}")
        
        # หา column ITEM_CODE, TEAM_NAME, STOCK_MIN และ STOCK_MAX
        item_col = None
        team_col = None
        stock_min_col = None
        stock_max_col = None
        
        for col in df_target.columns:
            col_upper = str(col).upper().strip()
            if 'ITEM' in col_upper and 'CODE' in col_upper:
                item_col = col
            elif 'TEAM' in col_upper and 'NAME' in col_upper:
                team_col = col
            elif 'STOCK' in col_upper and 'MIN' in col_upper:
                stock_min_col = col
            elif 'STOCK' in col_upper and 'MAX' in col_upper:
                stock_max_col = col
        
        if item_col is None or stock_min_col is None:
            print(f"⚠️  ไม่พบ column ที่ต้องการ: ITEM_CODE={item_col}, STOCK_MIN={stock_min_col}")
            df_stock['STOCK_MIN'] = 0
            df_stock['STOCK_MAX'] = 0
            return df_stock
        
        # lookup key = (ITEM_CODE, ลูกค้า, ทีม) — เป้าอยู่ที่ "แถวทะเบียน" ไม่ใช่ที่ตัวผ้า
        # ผ้ารหัสเดียวกันคนละลูกค้า/คนละทีม มี TARGET/MONTH คนละค่า จับผิดคู่ = เป้าเพี้ยนทั้งแถว
        cus_col = None
        type1_col = None
        target_col = None
        for col in df_target.columns:
            cu = str(col).upper().strip()
            if 'CUSTOMER' in cu:
                cus_col = col
            elif cu in ('TYPE 1', 'TYPE1'):
                type1_col = col
            elif 'TARGET' in cu and 'MONTH' in cu:
                target_col = col

        def _derive(row, team, kind):
            """คำนวณ STOCK MIN/MAX คืนจาก TARGET/MONTH เมื่อช่องในไฟล์ว่าง

            ไฟล์ Target Stock ตัวจริงเก็บคอลัมน์พวกนี้เป็น "สูตร Excel" → pandas อ่านได้ NaN
            ถ้าไม่คำนวณคืน STOCK_MIN จะเป็น 0 ทุกแถว แล้ว Planning จะข้ามการคิด trigger
            ผลิต core ทั้งหมด (เงื่อนไข _fsm > 0) โดยไม่มีอะไรฟ้อง
            สูตรต้องตรงกับ masters._recompute_target_stock (ที่หน้าเว็บใช้ตอนบันทึกไฟล์นี้):
              MIN = TARGET/MONTH / 4 ; SCM = TARGET/MONTH / 2
              MAX = (ทีม RTS และ Type 1 = CG) ? TARGET/MONTH : SCM * 1.5
            """
            if target_col is None:
                return 0
            try:
                tm = float(row.get(target_col, 0) or 0)
            except (TypeError, ValueError):
                return 0
            if tm != tm or tm <= 0:
                return 0
            if kind == 'STOCK_MIN':
                return tm / 4
            t1 = str(row.get(type1_col, '')).strip().upper() if type1_col else ''
            return tm if (team == 'RTS' and t1 == 'CG') else (tm / 2) * 1.5

        stock_lookup = {}
        for _, row in df_target.iterrows():
            item = str(row.get(item_col, '')).strip().upper()
            team = _ckey(row.get(team_col, '')) if team_col else ''
            cus = _ckey(row.get(cus_col, '')) if cus_col else ''
            stock_min = float(row.get(stock_min_col, 0) or 0)
            stock_max = float(row.get(stock_max_col, 0) or 0) if stock_max_col else 0
            if stock_min != stock_min or stock_min <= 0:        # NaN/ว่าง (เก็บเป็นสูตรในไฟล์)
                stock_min = _derive(row, team, 'STOCK_MIN')
            if stock_max != stock_max or stock_max <= 0:
                stock_max = _derive(row, team, 'STOCK_MAX')
            if item:
                stock_lookup[(item, cus, team)] = {
                    'STOCK_MIN': stock_min,
                    'STOCK_MAX': stock_max
                }
        
        # Merge STOCK_MIN และ STOCK_MAX เข้ากับ df_stock โดยจับทั้ง ITEM_CODE และ TEAM_NAME
        def get_stock_value(row, value_type):
            item = str(row.get('ITEM_CODE', '')).strip().upper()
            team = _ckey(row.get('TEAM_NAME', ''))
            cus = _ckey(row.get('CUSTOMER_NAME', ''))
            # ครบสามอย่างก่อน แล้วค่อยผ่อนลงตามข้อมูลที่มี (ไฟล์เก่าอาจไม่มีลูกค้า/ทีม)
            for key in ((item, cus, team), (item, cus, ''), (item, '', team), (item, '', '')):
                if key in stock_lookup:
                    return stock_lookup[key].get(value_type, 0)
            return 0
        
        df_stock['STOCK_MIN'] = df_stock.apply(lambda row: get_stock_value(row, 'STOCK_MIN'), axis=1)
        df_stock['STOCK_MAX'] = df_stock.apply(lambda row: get_stock_value(row, 'STOCK_MAX'), axis=1)
        
        # คำนวณ Stock 5 Week = STOCK_MIN * 5
        df_stock['Stock 5 Week'] = df_stock['STOCK_MIN'].apply(
            lambda x: float(x) * 5 if x > 0 else 0
        )
        
        matched_min = (df_stock['STOCK_MIN'] > 0).sum()
        matched_max = (df_stock['STOCK_MAX'] > 0).sum()
        print(f"✓ เพิ่ม column STOCK_MIN: {matched_min}/{len(df_stock)} items")
        print(f"✓ เพิ่ม column STOCK_MAX: {matched_max}/{len(df_stock)} items")
        print(f"✓ เพิ่ม column Stock 5 Week: คำนวณจาก STOCK_MIN × 5")
        
        return df_stock
        
    except Exception as e:
        print(f"⚠️  เกิดข้อผิดพลาดในการโหลด STOCK_MIN: {str(e)}")
        df_stock['STOCK_MIN'] = 0
        return df_stock


def add_outstanding_from_booking(df_stock):
    """
    เพิ่ม column OUTSTANDING จากไฟล์ Booking
    - OUTSTANDING_W{week+1}: week ถัดไป (week 17)
    - OUTSTANDING_W{week+2}: week ถัดไปอีก 1 week (week 18)
    - Inventory: BALANCE_KG + OUTSTANDING 2 weeks ถัดไป
    
    Returns:
        DataFrame with OUTSTANDING columns
    """
    # หา week ปัจจุบัน
    current_year, current_week = get_current_week()
    next_week_1 = current_week + 1
    next_week_2 = current_week + 2
    
    print(f"\n=== เพิ่มข้อมูล OUTSTANDING จาก Booking ===")
    print(f"Week ปัจจุบัน: {current_year}-W{current_week:02d} (ไม่นับ)")
    print(f"Week ถัดไป 1: {current_year}-W{next_week_1:02d}")
    print(f"Week ถัดไป 2: {current_year}-W{next_week_2:02d}")
    
    # หาไฟล์ Booking (relative path)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    booking_folder = os.path.join(script_dir, "Booking")
    booking_file = None
    
    # ค้นหาไฟล์ทุกประเภทในโฟลเดอร์ Booking (.xls, .xlsx, .csv, .txt)
    if os.path.exists(booking_folder):
        valid_extensions = ['.xls', '.xlsx', '.csv', '.txt', '.tsv']
        for file in os.listdir(booking_folder):
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in valid_extensions:
                booking_file = os.path.join(booking_folder, file)
                print(f"พบไฟล์ Booking: {file}")
                break
    
    if booking_file is None or not os.path.exists(booking_file):
        print(f"✗ ไม่พบไฟล์ Booking ใน {booking_folder}")
        return df_stock
    
    try:
        # ลองอ่านไฟล์ Booking ด้วยหลายวิธี
        df_booking = None
        
        # วิธีที่ 1: ลอง openpyxl (สำหรับ .xlsx)
        try:
            df_booking = pd.read_excel(booking_file, engine='openpyxl')
            print(f"อ่านด้วย openpyxl engine")
        except:
            pass
        
        # วิธีที่ 2: ลอง xlrd (สำหรับ .xls เก่า)
        if df_booking is None:
            try:
                df_booking = pd.read_excel(booking_file, engine='xlrd')
                print(f"อ่านด้วย xlrd engine")
            except:
                pass
        
        # วิธีที่ 3: ลองอ่านเป็น CSV/text file
        if df_booking is None:
            for encoding in ['tis-620', 'cp874', 'windows-1252', 'latin1', 'utf-8']:
                try:
                    df_booking = pd.read_csv(booking_file, sep='\t', encoding=encoding, on_bad_lines='skip')
                    print(f"อ่านเป็น tab-delimited file (encoding: {encoding})")
                    break
                except:
                    continue
        
        # วิธีที่ 4: ลอง HTML
        if df_booking is None:
            try:
                with open(booking_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(1000)
                    if '<html' in content.lower() or '<table' in content.lower():
                        print("ไฟล์เป็น HTML format, กำลังอ่านด้วย read_html...")
                        df_list = pd.read_html(booking_file)
                        df_booking = df_list[0] if df_list else None
                        print(f"อ่านด้วย read_html")
            except:
                pass
        
        if df_booking is None:
            print(f"✗ ไม่สามารถอ่านไฟล์ Booking ได้")
            print(f"กรุณาแปลงไฟล์เป็น .xlsx ก่อน หรือตรวจสอบ format")
            return df_stock
        
        print(f"✓ อ่านไฟล์ Booking: {len(df_booking)} แถว, {len(df_booking.columns)} columns")
        print(f"Columns: {df_booking.columns.tolist()[:15]}")
        
        # หา column ที่มี ITEM_CODE
        item_col = None
        for col in df_booking.columns:
            if 'ITEM' in str(col).upper() and 'CODE' in str(col).upper():
                item_col = col
                break
        
        if item_col is None:
            print("✗ ไม่พบ column ITEM_CODE ในไฟล์ Booking")
            return df_stock
        
        # หา column OUTSTANDING
        outstanding_col = None
        for col in df_booking.columns:
            if 'OUTSTANDING' in str(col).upper():
                outstanding_col = col
                break
        
        if outstanding_col is None:
            print("✗ ไม่พบ column OUTSTANDING ในไฟล์ Booking")
            return df_stock
        
        # หา column KP_WEIGHT
        kp_weight_col = None
        for col in df_booking.columns:
            if 'KP_WEIGHT' in str(col).upper() or 'KP WEIGHT' in str(col).upper():
                kp_weight_col = col
                break
        
        if kp_weight_col is None:
            print("✗ ไม่พบ column KP_WEIGHT ในไฟล์ Booking")
            return df_stock
        
        # หา column TEAM_NAME
        team_col = None
        for col in df_booking.columns:
            if 'TEAM' in str(col).upper() and 'NAME' in str(col).upper():
                team_col = col
                break
        
        # หา column WEEK
        week_col = None
        for col in df_booking.columns:
            if 'WEEK' in str(col).upper() and 'YEAR' not in str(col).upper():
                week_col = col
                break
        
        print(f"\nใช้ columns: ITEM={item_col}, TEAM={team_col}, OUTSTANDING={outstanding_col}, KP_WEIGHT={kp_weight_col}, WEEK={week_col}")
        
        # Filter ตาม week
        if week_col:
            # Filter week ถัดไป 1 (week 17)
            if team_col:
                df_week1 = df_booking[df_booking[week_col] == next_week_1][[item_col, team_col, outstanding_col]].copy()
                df_week1.columns = ['ITEM_CODE', 'TEAM_NAME', 'OUTSTANDING']
                # Sum OUTSTANDING ตาม ITEM_CODE และ TEAM_NAME
                df_week1_sum = df_week1.groupby(['ITEM_CODE', 'TEAM_NAME'], as_index=False)['OUTSTANDING'].sum()
                df_week1_sum.columns = ['ITEM_CODE', 'TEAM_NAME', f'OUTSTANDING_W{next_week_1:02d}']
            else:
                df_week1 = df_booking[df_booking[week_col] == next_week_1][[item_col, outstanding_col]].copy()
                df_week1.columns = ['ITEM_CODE', 'OUTSTANDING']
                df_week1_sum = df_week1.groupby('ITEM_CODE', as_index=False)['OUTSTANDING'].sum()
                df_week1_sum.columns = ['ITEM_CODE', f'OUTSTANDING_W{next_week_1:02d}']
            print(f"Week {next_week_1}: {len(df_week1)} แถว → {len(df_week1_sum)} items (หลัง sum)")
            
            # Filter week ถัดไป 2 (week 18) - ใช้ KP_WEIGHT แทน OUTSTANDING
            if team_col:
                df_week2 = df_booking[df_booking[week_col] == next_week_2][[item_col, team_col, kp_weight_col]].copy()
                df_week2.columns = ['ITEM_CODE', 'TEAM_NAME', 'KP_WEIGHT']
                # Sum KP_WEIGHT ตาม ITEM_CODE และ TEAM_NAME
                df_week2_sum = df_week2.groupby(['ITEM_CODE', 'TEAM_NAME'], as_index=False)['KP_WEIGHT'].sum()
                df_week2_sum.columns = ['ITEM_CODE', 'TEAM_NAME', f'Knit_Planning_W{next_week_2:02d}']
            else:
                df_week2 = df_booking[df_booking[week_col] == next_week_2][[item_col, kp_weight_col]].copy()
                df_week2.columns = ['ITEM_CODE', 'KP_WEIGHT']
                df_week2_sum = df_week2.groupby('ITEM_CODE', as_index=False)['KP_WEIGHT'].sum()
                df_week2_sum.columns = ['ITEM_CODE', f'Knit_Planning_W{next_week_2:02d}']
            print(f"Week {next_week_2}: {len(df_week2)} แถว → {len(df_week2_sum)} items (หลัง sum, ใช้ KP_WEIGHT)")
            
            # Merge กับ df_stock
            if team_col:
                df_stock = df_stock.merge(df_week1_sum, on=['ITEM_CODE', 'TEAM_NAME'], how='left')
                df_stock = df_stock.merge(df_week2_sum, on=['ITEM_CODE', 'TEAM_NAME'], how='left')
            else:
                df_stock = df_stock.merge(df_week1_sum, on='ITEM_CODE', how='left')
                df_stock = df_stock.merge(df_week2_sum, on='ITEM_CODE', how='left')
            
            # เติม 0 ให้กับ OUTSTANDING ที่เป็น NaN
            df_stock[f'OUTSTANDING_W{next_week_1:02d}'] = df_stock[f'OUTSTANDING_W{next_week_1:02d}'].fillna(0)
            df_stock[f'Knit_Planning_W{next_week_2:02d}'] = df_stock[f'Knit_Planning_W{next_week_2:02d}'].fillna(0)
            
            # คำนวณ Inventory = BALANCE_KG + OUTSTANDING ทั้ง 2 weeks ถัดไป
            df_stock['Inventory'] = (
                df_stock['BALANCE_KG'] +
                df_stock[f'OUTSTANDING_W{next_week_1:02d}'] +
                df_stock[f'Knit_Planning_W{next_week_2:02d}']
            )

            print(f"✓ เพิ่ม column OUTSTANDING_W{next_week_1:02d} และ Knit_Planning_W{next_week_2:02d}")
            print(f"✓ เพิ่ม column Inventory = BALANCE_KG + OUTSTANDING_W{next_week_1:02d} + Knit_Planning_W{next_week_2:02d}")
        else:
            print("⚠️  ไม่พบ column WEEK - ไม่สามารถ filter ตาม week ได้")
        
        return df_stock
        
    except Exception as e:
        print(f"✗ เกิดข้อผิดพลาด: {str(e)}")
        import traceback
        traceback.print_exc()
        return df_stock


def download_target_stock_file():
    target_path = Path(os.path.expandvars(_cfg["paths"]["target_stock"]))
    if target_path.exists():
        print(f"\n✓ พบไฟล์ Target Stock: {target_path}")
        return str(target_path)
    print(f"\n✗ ไม่พบไฟล์ Target Stock: {target_path}")
    print("⚠️  กรุณาตรวจสอบ path ใน config.ini [paths] target_stock")
    return None


def read_target_item_codes(target_file=None):
    """
    อ่าน Item code จากไฟล์ Target Stock
    Returns: list of item codes
    """
    if target_file is None:
        target_file = str(_APP_DIR / "Estimate_Core" / "Target_Stock.xlsx")
    
    if not os.path.exists(target_file):
        print(f"ไม่พบไฟล์ Target Stock: {target_file}")
        return None
    
    try:
        # อ่านไฟล์ Target Stock (สมมติว่า Item code อยู่ใน column แรก)
        df = pd.read_excel(target_file)
        print(f"\nอ่านไฟล์ Target Stock: {len(df)} แถว")
        print(f"Columns: {df.columns.tolist()}")
        
        # หา column ที่มีชื่อเกี่ยวกับ Item code
        item_col = None
        for col in df.columns:
            if 'ITEM' in str(col).upper() or 'CODE' in str(col).upper():
                item_col = col
                break
        
        if item_col is None:
            print("ไม่พบ column Item code ในไฟล์")
            print("กรุณาระบุชื่อ column ที่ต้องการ")
            return None
        
        # ดึง Item code ที่ไม่ซ้ำและไม่เป็น null
        item_codes = df[item_col].dropna().unique().tolist()
        print(f"\nพบ Item code ทั้งหมด: {len(item_codes)} รายการ")
        print(f"ตัวอย่าง Item code 10 รายการแรก: {item_codes[:10]}")
        
        return item_codes
        
    except Exception as e:
        print(f"เกิดข้อผิดพลาดในการอ่านไฟล์ Target Stock: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def _ckey(v) -> str:
    """ชื่อลูกค้า/ทีม -> คีย์เทียบ (ยุบช่องว่างซ้ำ + พิมพ์ใหญ่) เพราะชื่อไทยในไฟล์เว้นวรรคไม่เท่ากันได้"""
    return " ".join(str(v or "").split()).upper()


def read_target_keys(target_file=None):
    """ทะเบียน Core -> set ของ (ITEM, ลูกค้า, ทีม)

    core ตัดสินด้วยสามอย่างนี้พร้อมกัน — ผ้ารหัสเดียวกันที่ผูกลูกค้าอื่นหรืออยู่คนละทีม
    เป็นของออเดอร์เขา ไม่ใช่สต๊อก core ของแถวนั้น (แต่ละแถวมี TARGET/MONTH ของตัวเอง)
    """
    if target_file is None:
        target_file = str(_APP_DIR / "Estimate_Core" / "Target_Stock.xlsx")
    if not os.path.exists(target_file):
        return set()
    try:
        df = pd.read_excel(target_file)
        df.columns = [str(c).strip() for c in df.columns]
    except Exception as e:
        print(f"อ่านทะเบียน Core ไม่ได้: {e}")
        return set()

    cols = {str(c).upper(): c for c in df.columns}
    ic = cols.get('ITEM CODE') or cols.get('ITEM_CODE') or cols.get('ITEMCODE')
    cc = cols.get('CUSTOMER')
    tc = cols.get('TEAM NAME') or cols.get('TEAM_NAME')
    if ic is None:
        return set()

    keys = set()
    for _, r in df.iterrows():
        item = str(r.get(ic, '')).strip().upper()
        if not item or item in ('NAN', 'NONE'):
            continue
        keys.add((item,
                  _ckey(r.get(cc, '')) if cc else '',
                  _ckey(r.get(tc, '')) if tc else ''))
    print(f"ทะเบียน Core (ผ้า+ลูกค้า+ทีม): {len(keys)} แถว")
    return keys


def read_stock_data(target_item_codes=None, target_keys=None):
    """
    อ่านข้อมูล Stock จากไฟล์ STOCK_Data 1.xlsx
    Sheet: Raw
    Columns: TEAM_NAME, ITEM_CODE, QA_REASON, QA_REMARK, BALANCE_KG

    Filter:
    - TEAM_NAME: RTS, NYK1-1
    - QA_REASON: blank
    - QA_REMARK: blank
    - ITEM_CODE: เฉพาะที่อยู่ใน Target Stock (ถ้ามี)
    - BALANCE_KG: >= 100 (ยอดคงเหลือหลังหักจอง ไม่ใช่ ONHAND_KG ที่ยังไม่หัก)
    """
    
    # ค้นหาไฟล์ STOCK ในโฟลเดอร์ Stock (relative path)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    stock_folder = os.path.join(script_dir, "Stock")
    stock_file = None
    
    if os.path.exists(stock_folder):
        for file in os.listdir(stock_folder):
            if "STOCK" in file.upper() and file.endswith(".xlsx"):
                stock_file = os.path.join(stock_folder, file)
                print(f"พบไฟล์ Stock: {file}")
                break
    
    if stock_file is None or not os.path.exists(stock_file):
        print(f"✗ ไม่พบไฟล์ STOCK ใน {stock_folder}")
        return None
    
    try:
        # อ่านชีทแรก — ไฟล์ที่ View_Stock.py เขียนมีชีทเดียว (ชื่อชีท = ชื่อ view ไม่อ้างในโค้ด)
        df = pd.read_excel(stock_file, sheet_name=0)
        
        print(f"อ่านข้อมูลทั้งหมด: {len(df)} แถว")
        print(f"Columns ที่มี: {df.columns.tolist()}")
        
        # เลือกเฉพาะ columns ที่ต้องการ
        # ยอดสต๊อกยึด BALANCE_KG = ของที่เหลือหลังหักยอดจอง (RESERVE_KG) แล้ว
        # ไม่ใช่ ONHAND_KG ที่เป็นของในคลังก่อนหักจอง — ของที่ถูกจองไปแล้วเอามาวางแผนซ้ำไม่ได้
        required_columns = ['TEAM_NAME', 'CUSTOMER_NAME', 'ITEM_CODE', 'QA_REASON', 'QA_REMARK', 'BALANCE_KG']

        # ตรวจสอบว่ามี columns ที่ต้องการหรือไม่
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"ไม่พบ columns: {missing_cols}")
            return None

        # เลือกเฉพาะ columns ที่ต้องการ
        df_filtered = df[required_columns].copy()

        # จองเกินของที่มี → BALANCE_KG ติดลบ ปัดเป็น 0 ไม่งั้นไปหักยอดล็อตอื่นในก้อนเดียวกัน
        df_filtered['BALANCE_KG'] = pd.to_numeric(
            df_filtered['BALANCE_KG'], errors='coerce').fillna(0).clip(lower=0)

        # Filter TEAM_NAME เอาเฉพาะ RTS และ NYK1-1
        # กรองด้วยทะเบียน Core ทั้งชุด (ผ้า + ลูกค้า + ทีม) — ล็อตที่ไม่ครบสามอย่างเป็นของก้อนอื่น
        # เดิมกรองด้วยชื่อทีมที่ hard-code ไว้ ซึ่งจะเงียบทันทีที่ทะเบียนเพิ่มทีมใหม่ และยังปล่อย
        # ล็อตของลูกค้าอื่นที่บังเอิญอยู่ทีมเดียวกันหลุดเข้ามาเป็นสต๊อก core
        if target_keys:
            _k = list(zip(df_filtered['ITEM_CODE'].astype(str).str.strip().str.upper(),
                          df_filtered['CUSTOMER_NAME'].map(_ckey),
                          df_filtered['TEAM_NAME'].map(_ckey)))
            df_filtered = df_filtered[[t in target_keys for t in _k]]
        else:
            df_filtered = df_filtered[df_filtered['TEAM_NAME'].isin(['RTS', 'NYK1-1'])]
        print(f"หลัง filter TEAM_NAME (RTS, NYK1-1): {len(df_filtered)} แถว")
        
        # Filter QA_REASON และ QA_REMARK เอาเฉพาะค่า blank (NaN หรือ empty string)
        df_filtered = df_filtered[
            (df_filtered['QA_REASON'].isna() | (df_filtered['QA_REASON'] == '')) &
            (df_filtered['QA_REMARK'].isna() | (df_filtered['QA_REMARK'] == ''))
        ]
        print(f"หลัง filter QA_REASON และ QA_REMARK (blank): {len(df_filtered)} แถว")
        
        # Filter ตาม Item code จาก Target Stock (ถ้ามี)
        if target_item_codes is not None and len(target_item_codes) > 0:
            df_filtered = df_filtered[df_filtered['ITEM_CODE'].isin(target_item_codes)]
            print(f"หลัง filter ตาม Target Stock Item codes: {len(df_filtered)} แถว")
        
        # Filter เฉพาะ BALANCE_KG >= 100 ก่อน sum (ล็อตที่เหลือน้อยกว่านี้คือเศษ ขายไม่ได้)
        before_filter = len(df_filtered)
        df_filtered = df_filtered[df_filtered['BALANCE_KG'] >= 100]
        removed = before_filter - len(df_filtered)
        print(f"หลัง filter BALANCE_KG >= 100: {len(df_filtered)} แถว (ตัดออก {removed} แถว)")

        # Sum ข้อมูลที่มี TEAM_NAME และ ITEM_CODE เหมือนกัน (หลังจาก filter >= 100 แล้ว)
        print(f"\nก่อน sum: {len(df_filtered)} แถว")
        df_grouped = df_filtered.groupby(['TEAM_NAME', 'CUSTOMER_NAME', 'ITEM_CODE'], as_index=False).agg({
            'BALANCE_KG': 'sum',
            'QA_REASON': 'first',
            'QA_REMARK': 'first'
        })
        print(f"หลัง sum (group by TEAM_NAME, ITEM_CODE): {len(df_grouped)} แถว")
        
        # แสดงข้อมูลตัวอย่าง
        print("\n=== ข้อมูลตัวอย่าง 10 แถวแรก (หลัง filter และ sum) ===")
        print(df_grouped.head(10))
        
        print(f"\n=== สรุป ===")
        print(f"จำนวนแถวทั้งหมด: {len(df_grouped)}")
        print(f"TEAM_NAME ที่มี: {df_grouped['TEAM_NAME'].unique()}")
        print(f"จำนวน ITEM_CODE ที่ไม่ซ้ำ: {df_grouped['ITEM_CODE'].nunique()}")
        print(f"รวม BALANCE_KG ทั้งหมด: {df_grouped['BALANCE_KG'].sum():.2f} Kg")
        
        return df_grouped
        
    except Exception as e:
        print(f"เกิดข้อผิดพลาด: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("=" * 80)
    print("Stock Data Processing with Target Stock Filter")
    print("=" * 80)
    
    # Step 1: ดาวน์โหลด/ตรวจสอบไฟล์ Target Stock
    target_file = download_target_stock_file()
    
    # Step 2: อ่าน Item code จาก Target Stock
    target_item_codes = None
    if target_file:
        target_item_codes = read_target_item_codes(target_file)
    
    # Step 3: อ่านและ filter ข้อมูล Stock
    print("\n" + "=" * 80)
    print("กำลังประมวลผลข้อมูล Stock...")
    print("=" * 80)
    target_keys = read_target_keys(target_file) if target_file else None
    stock_data = read_stock_data(target_item_codes, target_keys)
    
    # Step 4: เพิ่มข้อมูล OUTSTANDING จาก Booking
    if stock_data is not None:
        stock_data = add_outstanding_from_booking(stock_data)
    
    # Step 5: เพิ่มข้อมูล STOCK_MIN จาก Target Stock
    if stock_data is not None and target_file:
        stock_data = add_stock_min_from_target(stock_data, target_file)
    
    # Step 6: บันทึกผลลัพธ์
    if stock_data is not None:
        script_dir = str(_APP_DIR)
        output_file = os.path.join(script_dir, "data_plan", "filtered_stock_data.xlsx")
        stock_data.to_excel(output_file, index=False)
        print(f"\n{'=' * 80}")
        print(f"✓ บันทึกข้อมูลที่กรองแล้วไปที่: {output_file}")
        print(f"{'=' * 80}")
    else:
        print("\n✗ ไม่สามารถประมวลผลข้อมูลได้")
