"""
McTotal.py — ตรรกะ "จำนวนเครื่องรายสัปดาห์" (ชีท MC_Total) ที่ AVA_MC ใช้เขียนทับ SUMMARY_MC_REMAIN

แยกออกจาก AVA_MC.py (ซึ่งเป็นสคริปต์ top-level รันทั้งไฟล์ตอน import) ให้เป็นฟังก์ชันล้วน
ไม่อ่านไฟล์ → ทดสอบได้ด้วย tests/test_mc_total_pile.py

กติกา (ต้องตรงกับหน้าเครื่องจักรบนเว็บ):
  - ยอดที่ user กรอก = เครื่อง "ทั้งกอง" ของสัปดาห์นั้น รวมเครื่องที่ MC Special กันไว้ให้
    POLY/COTTON แล้ว (หน่วยเดียวกับ Total MC ของทะเบียน Master MC)
  - TOTAL_MC ของแถวพูลในตาราง = "พูลปกติ" ที่หักเครื่องกันไว้ออกแล้ว และพูลย่อยไม่มีแถวของตัวเอง
    → พูลปกติ = ยอดที่กรอก − เครื่องกันไว้ ; ไม่หัก = เครื่องกันไว้ถูกนับซ้ำ
    (เคสจริง PHET DOUBLE-34 28G: กรอก 14, กัน COTTON 4 → เคยได้ ปกติ 14 + COTTON 4 = 18)
  - ต้องเรียก "หลัง" เติมสัปดาห์อนาคตให้ครบ horizon — สัปดาห์ที่ยังไม่มี booking ไม่มีแถว
    ก่อนเติม ค่าที่ตั้งไว้จะถูกข้ามเงียบ ๆ (เคสจริง W53/2026)
"""
import pandas as pd

RESERVED_SUFFIXES = (":POLY", ":COTTON")
_ROW_KEY = "\x00row"     # คีย์ของแถว non-pool (MC_POOL ว่าง) — ไม่มีพี่น้อง ถือเป็นก้อนของตัวเอง


def split_total(new_total, olds):
    """กระจาย new_total ให้แต่ละพูลตามสัดส่วนของ olds (largest remainder)
    olds รวมกัน = 0 → กระจายเท่า ๆ กัน"""
    n = len(olds)
    if n == 1:
        return [new_total]
    base = sum(olds)
    if base <= 0:
        shares = [new_total / n] * n
    else:
        shares = [new_total * o / base for o in olds]
    out = [int(s) for s in shares]
    rest = new_total - sum(out)
    # เศษที่เหลือ → ให้พูลที่เศษทศนิยมมากสุดก่อน (เท่ากันให้พูลใหญ่กว่า)
    order = sorted(range(n), key=lambda i: (shares[i] - out[i], olds[i]), reverse=True)
    for i in range(rest):
        out[order[i % n]] += 1
    return out


def reserved_of(pool, total_by_type) -> int:
    """เครื่องที่ MC Special กันไว้ให้ POLY/COTTON ของพูลนี้ (พูลย่อย pool:POLY / pool:COTTON)"""
    if not pool or pool.startswith(_ROW_KEY):
        return 0
    return sum(int(total_by_type.get(f"{pool}{sfx}", 0) or 0) for sfx in RESERVED_SUFFIXES)


def split_pile(new_total, normals, reserveds):
    """ยอดทั้งกองที่กรอก → (ยอดเต็มของแต่ละพูล, ยอดพูลปกติของแต่ละพูล)

    กระจายตามสัดส่วน "ยอดเต็ม" เดิม (ปกติ + กันไว้) แล้วหักเครื่องกันไว้ออกจากส่วนของพูลนั้น
    ได้น้อยกว่าที่กันไว้ → พูลปกติ = 0 (เครื่องกันไว้ยังนับตามชีท MC Special)"""
    fulls = [n + r for n, r in zip(normals, reserveds)]
    news_full = split_total(int(new_total), fulls)
    return news_full, [max(0, n - r) for n, r in zip(news_full, reserveds)]


def _frame_keys(summary, norm_gauge):
    def _txt(col):
        if col not in summary.columns:
            return pd.Series("", index=summary.index)
        return summary[col].astype(str).str.strip().replace("nan", "")
    return (
        summary["MC_CAT"].astype(str).str.strip().str.upper(),
        summary["GUAGE"].apply(norm_gauge),
        _txt("FACTORY").str.upper(),
        _txt("MC_POOL"),
        summary["WEEK"].astype(int),
    )


def apply_override(summary, overrides, mc_to_pool, total_by_type, norm_gauge, log=print):
    """เขียนทับ TOTAL_MC / TOTAL_MC_REMAIN ของ summary (แก้ในที่) ตามชีท MC_Total

    overrides     : {(YW, cat_upper, gauge_norm, factory_upper|"", mc_upper|""): ยอดทั้งกอง}
    mc_to_pool    : {(MC_upper, gauge_norm): pool_key}
    total_by_type : {pool_key: จำนวนเครื่อง} — ใช้หาเครื่องกันไว้ของพูล (pool:POLY / pool:COTTON)

    หลายพูล/โรงงาน → กระจายตามสัดส่วนยอดเต็มเดิม เพราะพูลแยกกันใช้แทนกันไม่ได้
    ⚠️ หน่วยของการกระจายคือ "พูล" ไม่ใช่ "แถว" — พูลเดียวกันโผล่ได้หลายแถว (คนละ cat/เกจ)
       โดยถือยอดเดียวกันซ้ำ ๆ ทุกแถวของพูลจึงต้องถูกเขียนค่าใหม่พร้อมกัน

    คืน (delta, applied, skipped)
      delta   : {(YW, cat, gauge): ส่วนต่างเทียบยอดเต็มเดิม} ให้ชีทรายงาน AVA_MC ปรับตาม
      applied : [{key, week, label, total, pools, clamped}] รายการที่เขียนแล้ว (ให้ verify ใช้)
      skipped : [(key, label)] รายการที่หาแถวไม่เจอ
    """
    cat_u, g_n, fac_u, pool_s, week_i = _frame_keys(summary, norm_gauge)
    delta, applied, skipped = {}, [], []
    for key, ov_total in sorted(overrides.items()):
        ov_w, ov_cat, ov_g, ov_fac, ov_mc = key
        mask = (week_i == ov_w) & (cat_u == ov_cat) & (g_n == ov_g)
        if ov_fac:
            mask &= (fac_u == ov_fac)
        # ระบุ MC = ปรับเฉพาะพูลของเครื่องนั้น (เช่น SKP ไม่แตะพูล SKPLE/SKPTA)
        ov_pool = ""
        if ov_mc:
            ov_pool = mc_to_pool.get((ov_mc, ov_g), "")
            if not ov_pool:
                log(f"⚠️ MC_Total: ไม่รู้จักเครื่อง {ov_mc} เกจ {ov_g} ใน Master MC → ข้าม")
                skipped.append((key, f"W{ov_w} {ov_cat} เกจ {ov_g} เครื่อง {ov_mc}"))
                continue
            mask &= (pool_s == ov_pool)
        lbl = (f"W{ov_w} {ov_cat} เกจ {ov_g}"
               f"{' เครื่อง ' + ov_mc + ' (พูล ' + ov_pool + ')' if ov_mc else ''}"
               f"{' โรงงาน ' + ov_fac if ov_fac else ''}")
        idx = list(summary.index[mask])
        if not idx:
            log(f"⚠️ MC_Total: ไม่พบแถว {lbl} → ข้าม")
            skipped.append((key, lbl))
            continue
        # จัดกลุ่มแถวตาม MC_POOL ก่อน: แถวที่พูลเดียวกันถือ "ยอดของทั้งพูล" ซ้ำกัน
        # (พูลข้าม cat เช่น "PHET|IIP 20G , RL 18G" โผล่ทั้ง DOUBLE-36 20 และ DOUBLE-40 18)
        # → ต้องนับยอดเดิมพูลละครั้ง ไม่งั้นถูกหั่นซ้ำ และต้องเขียนค่าใหม่ให้ครบทุกแถวของพูล
        pool_groups: dict = {}
        for i in idx:
            p = str(pool_s.at[i]).strip()
            pool_groups.setdefault(p or f"{_ROW_KEY}{i}", []).append(i)
        gkeys = list(pool_groups.keys())
        olds = [int(summary.at[pool_groups[k][0], "TOTAL_MC"] or 0) for k in gkeys]
        rsvs = [reserved_of(k, total_by_type) for k in gkeys]
        news_full, news = split_pile(ov_total, olds, rsvs)
        clamped = False
        for k, nf, r in zip(gkeys, news_full, rsvs):
            if nf < r:
                clamped = True
                log(f"⚠️ MC_Total: {lbl} พูล {k} ได้ {nf} เครื่อง น้อยกว่าที่ MC Special "
                    f"กันไว้ {r} → พูลปกติเหลือ 0 (เครื่องกันไว้ยังนับ {r} ตามชีท MC Special)")
        for k, new in zip(gkeys, news):
            # แถวพี่น้องของพูลเดียวกันในสัปดาห์นั้น (คนละ cat/เกจ) ต้องได้ยอดเดียวกันด้วย
            targets = set(pool_groups[k])
            if not k.startswith(_ROW_KEY):
                targets |= set(summary.index[(week_i == ov_w) & (pool_s == k)])
            for i in targets:
                summary.at[i, "TOTAL_MC"] = new
                summary.at[i, "TOTAL_MC_REMAIN"] = new - int(summary.at[i, "MC_USE_CEIL"] or 0)
        fulls = [o + r for o, r in zip(olds, rsvs)]
        dk = (ov_w, ov_cat, ov_g)
        # ส่วนต่างเทียบ "ยอดเต็ม" (ปกติ + กันไว้) — ชีทรายงาน AVA_MC ตั้งต้นจาก Total mc ของทะเบียน
        delta[dk] = delta.get(dk, 0) + int(ov_total) - sum(fulls)
        applied.append({"key": key, "week": ov_w, "label": lbl, "total": int(ov_total),
                        "pools": gkeys, "clamped": clamped})
        log(f"🔢 MC_Total: {lbl} → {sum(fulls)} เป็น {int(ov_total)} เครื่อง"
            f"{' (กระจาย ' + '+'.join(str(n) for n in news_full) + ')' if len(gkeys) > 1 else ''}"
            f"{' — ในนี้กันไว้ POLY/COTTON ' + str(sum(rsvs)) + ' → พูลปกติ ' + str(sum(news)) if sum(rsvs) else ''}")
    return delta, applied, skipped


def verify(summary, applied, skipped, total_by_type, norm_gauge, horizon=()):
    """ตัวตรวจหลังเขียนทับ — คืน list ข้อความของสิ่งที่ผิด (ว่าง = ถูกทั้งหมด)

    1. ค่าที่ตั้งไว้ของสัปดาห์ใน horizon ต้องถูกใช้ทุกรายการ (ห้ามหาแถวไม่เจอ)
    2. ทุกรายการ: ผลรวม (พูลปกติ + เครื่องกันไว้) ของพูลที่รายการนั้นคุม = ยอดที่กรอก
       ข้ามรายการที่พูลถูกรายการอื่นของสัปดาห์เดียวกันเขียนทับ (ตั้งใจให้แถวเจาะจงกว่าชนะ)
       และรายการที่ได้น้อยกว่าเครื่องกันไว้ (พูลปกติถูกตรึงที่ 0 ยอดรวมจึงมากกว่าที่กรอกได้)
    """
    problems = []
    hz = set(int(w) for w in horizon)
    for key, lbl in skipped:
        if int(key[0]) in hz:
            problems.append(f"{lbl}: ตั้งจำนวนเครื่องไว้แต่ไม่ถูกใช้ (ไม่พบแถวของสัปดาห์นี้ในตารางเครื่อง)")
    _c, _g, _f, pool_s, week_i = _frame_keys(summary, norm_gauge)
    touch: dict = {}                       # (week, pool) → จำนวนรายการที่เขียนพูลนี้
    for a in applied:
        for k in a["pools"]:
            touch[(a["week"], k)] = touch.get((a["week"], k), 0) + 1
    for a in applied:
        if a["clamped"] or any(touch[(a["week"], k)] > 1 for k in a["pools"]):
            continue
        got = 0
        for k in a["pools"]:
            if k.startswith(_ROW_KEY):
                rows = [int(k[len(_ROW_KEY):])]
            else:
                rows = list(summary.index[(week_i == a["week"]) & (pool_s == k)])
            vals = {int(summary.at[i, "TOTAL_MC"] or 0) for i in rows if i in summary.index}
            if len(vals) != 1:
                problems.append(f"{a['label']}: แถวของพูล {k} ถือยอดไม่เท่ากัน {sorted(vals)}")
                continue
            got += vals.pop() + reserved_of(k, total_by_type)
        if got != a["total"]:
            problems.append(f"{a['label']}: ตั้งไว้ {a['total']} เครื่อง แต่ตารางเครื่องได้ {got} "
                            f"(พูลปกติ + เครื่องกันไว้ POLY/COTTON)")
    return problems
