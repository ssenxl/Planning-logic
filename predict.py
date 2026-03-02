import argparse
import sys
import re
import joblib
import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
MODEL_DIR = Path("model")
MODEL_PATH = MODEL_DIR / "mc_group_model_svm.joblib"
ITEM_MAP_PATH = MODEL_DIR / "item_mc_map.joblib"

AUTO_THRESHOLD = 0.85      # ≥ 85% → AUTO
FALLBACK_THRESHOLD = 0.60  # 60–85% → FALLBACK
# < 60% → TRASH

_item_map_cache = None


# =========================
# UTILS
# =========================
def _load_item_map() -> dict:
    global _item_map_cache
    if _item_map_cache is not None:
        return _item_map_cache
    if ITEM_MAP_PATH.exists():
        _item_map_cache = joblib.load(ITEM_MAP_PATH)
    else:
        _item_map_cache = {}
    return _item_map_cache


def is_junk_text(text: str) -> bool:
    text = str(text).strip().lower()
    if not text:
        return True
    if len(text) < 3:
        return True
    if re.fullmatch(r"[a-z0-9]{3,}", text):
        return True
    if re.search(r"(asdf|qwer|zxcv|1234)", text):
        return True
    return False


def looks_like_real_item_code(text: str) -> bool:
    text = str(text).strip()
    if len(text) < 8:
        return False
    if not (re.search(r"[A-Za-z]", text) and re.search(r"\d", text)):
        return False
    if re.search(r"[^\w\-]", text):
        return False
    return True


# =========================
# CORE PREDICT
# =========================
def predict_mc_group(item_code: str, description: str = ""):
    item_code_clean = str(item_code).strip()

    # 1️⃣ Seen item → deterministic
    item_map = _load_item_map()
    if item_code_clean in item_map:
        return {
            "group": item_map[item_code_clean],
            "confidence": 1.0,
            "status": "SEEN_ITEM"
        }

    # 2️⃣ Junk input → TRASH
    if is_junk_text(description):
        if not looks_like_real_item_code(item_code_clean):
            return {
                "group": "TRASH_INPUT",
                "confidence": 0.0,
                "status": "TRASH"
            }

    # 3️⃣ ML prediction
    model = joblib.load(MODEL_PATH)

    X = pd.DataFrame([{
        "DESCRIPTION": str(description),
        "ITEM_PREFIX": item_code_clean[:6],
        "TYPE": "",
        "GUAGE": "",
        "CAT": "",
        "FACTORY": ""
    }])

    proba = model.predict_proba(X)[0]
    idx = proba.argmax()
    score = float(proba[idx])
    group = model.classes_[idx]

    # 4️⃣ Decision policy
    if score >= AUTO_THRESHOLD:
        status = "AUTO"
    elif score >= FALLBACK_THRESHOLD:
        status = "FALLBACK_NEED_REVIEW"
    else:
        return {
            "group": "TRASH_LOW_CONFIDENCE",
            "confidence": score,
            "status": "TRASH"
        }

    return {
        "group": group,
        "confidence": score,
        "status": status
    }


# =========================
# CLI
# =========================
def _cli():
    print("Enter ITEM_CODE (or 'exit' to quit)")
    print("DESCRIPTION is optional\n")

    while True:
        try:
            item_code = input("ITEM_CODE: ").strip()
        except KeyboardInterrupt:
            print("\nExit.")
            break

        if not item_code:
            continue
        if item_code.lower() in {"exit", "quit"}:
            break

        description = input("DESCRIPTION (press enter to skip): ").strip()

        result = predict_mc_group(item_code, description)

        if result["status"].startswith("TRASH"):
            print(f"⚠️ TRASH → {result['group']} ({result['confidence']*100:.2f}%)\n")
        elif result["status"] == "SEEN_ITEM":
            print(f"✅ {result['group']} (100.00%) [SEEN]\n")
        elif result["status"] == "AUTO":
            print(f"✅ {result['group']} ({result['confidence']*100:.2f}%) [AUTO]\n")
        else:
            print(f"⚠️ FALLBACK → {result['group']} ({result['confidence']*100:.2f}%)\n")


if __name__ == "__main__":
    _cli()
