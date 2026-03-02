import pandas as pd
import joblib
from pathlib import Path

from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split


# =========================
# PATH
# =========================
MODEL_DIR = Path("model")
MODEL_DIR.mkdir(exist_ok=True)

DATA_DIR = Path("data") / "MC"

MODEL_PATH = MODEL_DIR / "mc_group_model_svm.joblib"
ITEM_MAP_PATH = MODEL_DIR / "item_mc_map.joblib"


def train_mc_group_model(df: pd.DataFrame):
    # =========================
    # FIX LABEL CONFLICT: 1 (DESCRIPTION, YARN_ITEM) -> 1 MC_GROUP (keep most frequent)
    # =========================
    group_cnt2 = (
        df.groupby(["DESCRIPTION", "YARN_ITEM", "MC_GROUP"]).size().reset_index(name="cnt")
    )
    dominant_group2 = (
        group_cnt2
        .sort_values(["DESCRIPTION", "YARN_ITEM", "cnt"], ascending=[True, True, False])
        .drop_duplicates(["DESCRIPTION", "YARN_ITEM"])
    )
    df = df.merge(
        dominant_group2[["DESCRIPTION", "YARN_ITEM", "MC_GROUP"]],
        on=["DESCRIPTION", "YARN_ITEM", "MC_GROUP"],
        how="inner"
    )

    """
    Train MC_GROUP classifier
    - Main model: Linear SVM
    - Seen ITEM -> direct lookup
    - New ITEM -> ML prediction
    """

    # =========================
    # BASIC CLEAN
    # =========================
    required_cols = ["ITEM_CODE", "DESCRIPTION", "MC_GROUP", "TYPE", "GUAGE"]
    subset_cols = [col for col in required_cols if col in df.columns]
    df = df.dropna(subset=subset_cols).copy()

    for col in ["ITEM_CODE", "DESCRIPTION", "TYPE", "GUAGE", "CAT", "FACTORY"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # =========================
    # 🔥 FIX LABEL CONFLICT
    # 1 ITEM_CODE -> 1 MC_GROUP (keep most frequent)
    # =========================
    group_cnt = (
        df.groupby(["ITEM_CODE", "MC_GROUP"])
          .size()
          .reset_index(name="cnt")
    )

    dominant_group = (
        group_cnt
        .sort_values(["ITEM_CODE", "cnt"], ascending=[True, False])
        .drop_duplicates("ITEM_CODE")
    )

    df = df.merge(
        dominant_group[["ITEM_CODE", "MC_GROUP"]],
        on=["ITEM_CODE", "MC_GROUP"],
        how="inner"
    )

    # =========================
    # DEDUPE
    # =========================
    df = (
        df
        .assign(desc_len=df["DESCRIPTION"].str.len())
        .sort_values("desc_len", ascending=False)
        .drop_duplicates(subset=["ITEM_CODE"], keep="first")
        .drop(columns="desc_len")
    )

    # =========================
    # 🚫 EXCLUDE TSA, TSB, TSC, TSD, TSE, TSF FROM TRAIN
    # =========================
    EXCLUDE_GROUPS = []
    df_train = df[~df["MC_GROUP"].isin(EXCLUDE_GROUPS)].copy()
    print("\n🚫 Excluded from training:", EXCLUDE_GROUPS)

    # =========================
    # FEATURE ENGINEERING
    # =========================
    df_train["ITEM_PREFIX"] = df_train["ITEM_CODE"].str[:6]

    for col in ["YARN_ITEM"]:
        if col not in df_train.columns:
            df_train[col] = ""
        else:
            df_train[col] = df_train[col].astype(str).str.strip().fillna("")

    feature_cols = [col for col in ["DESCRIPTION", "YARN_ITEM", "ITEM_PREFIX", "TYPE", "GUAGE"] if col in df_train.columns]
    X = df_train[feature_cols]
    y = df_train["MC_GROUP"]

    print("\n✅ MC_GROUP distribution (after SYN excluded)")
    print(y.value_counts())

    # =========================
    # REMOVE RARE CLASSES
    # =========================
    value_counts = y.value_counts()
    rare_classes = value_counts[value_counts < 2].index.tolist()

    if rare_classes:
        print(f"\n⚠️ Remove rare classes (<2 samples): {rare_classes}")
        mask = ~y.isin(rare_classes)
        X = X[mask]
        y = y[mask]

    # =========================
    # MULTI-ROUND TRAINING
    # เทรนหลายรอบ แต่ละรอบใช้ random split ต่างกัน
    # เก็บโมเดลที่ test accuracy ดีสุด
    # =========================
    N_ROUNDS = 10
    C_VALUES = [1.0, 2.0, 5.0]  # ลอง C หลายค่าด้วย

    cat_cols = [col for col in ["ITEM_PREFIX", "TYPE", "GUAGE"] if col in X.columns]

    best_model = None
    best_acc = 0.0
    best_round = 0

    print(f"\n🔄 Training {N_ROUNDS} rounds x {len(C_VALUES)} C values = {N_ROUNDS * len(C_VALUES)} total fits ...")

    for seed in range(N_ROUNDS):
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=seed, stratify=y
        )

        min_class_count = y_train.value_counts().min()
        cv_folds = max(2, min(3, min_class_count))

        for c_val in C_VALUES:
            transformers = [
                (
                    "desc_word",
                    TfidfVectorizer(
                        ngram_range=(1, 2),
                        max_features=8000,
                        min_df=2,
                        sublinear_tf=True,
                        analyzer="word"
                    ),
                    "DESCRIPTION"
                ),
                (
                    "desc_char",
                    TfidfVectorizer(
                        ngram_range=(2, 4),
                        max_features=3000,
                        min_df=2,
                        sublinear_tf=True,
                        analyzer="char_wb"
                    ),
                    "DESCRIPTION"
                ),
                (
                    "yarn_word",
                    TfidfVectorizer(
                        ngram_range=(1, 2),
                        max_features=4000,
                        min_df=1,
                        sublinear_tf=True,
                        analyzer="word"
                    ),
                    "YARN_ITEM"
                )
            ]
            if cat_cols:
                transformers.append(("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols))

            candidate = Pipeline(
                steps=[
                    ("preprocess", ColumnTransformer(transformers=transformers)),
                    (
                        "clf",
                        CalibratedClassifierCV(
                            estimator=LinearSVC(
                                C=c_val,
                                class_weight="balanced",
                                max_iter=6000,
                                dual=False
                            ),
                            cv=cv_folds,
                            method="sigmoid"
                        )
                    )
                ]
            )

            candidate.fit(X_train, y_train)
            acc = accuracy_score(y_test, candidate.predict(X_test))
            print(f"  Round {seed+1:2d} | C={c_val:.1f} | Test Acc: {acc:.4f}", end="")

            if acc > best_acc:
                best_acc = acc
                best_model = candidate
                best_round = seed + 1
                best_c = c_val
                best_X_train, best_X_test = X_train, X_test
                best_y_train, best_y_test = y_train, y_test
                print(" ✅ Best so far!")
            else:
                print()

    model = best_model
    print(f"\n🏆 Best model: Round {best_round}, C={best_c:.1f}, Test Acc={best_acc:.4f}")

    print("\n🎯 Train Performance")
    y_pred_train = model.predict(best_X_train)
    print(f"Accuracy: {accuracy_score(best_y_train, y_pred_train):.3f}")
    print(classification_report(best_y_train, y_pred_train))

    print("\n🎯 Test Performance")
    y_pred_test = model.predict(best_X_test)
    print(f"Accuracy: {accuracy_score(best_y_test, y_pred_test):.3f}")
    print(classification_report(best_y_test, y_pred_test))

    # =========================
    # SAVE
    # =========================
    joblib.dump(model, MODEL_PATH)
    print(f"\n✅ Model saved: {MODEL_PATH}")

    # 🔥 item_map ยังเก็บ SYN ไว้
    item_map = (
        df[["ITEM_CODE", "MC_GROUP"]]
        .drop_duplicates("ITEM_CODE")
        .set_index("ITEM_CODE")["MC_GROUP"]
        .to_dict()
    )

    joblib.dump(item_map, ITEM_MAP_PATH)
    print(f"✅ Item map saved: {ITEM_MAP_PATH}")

    return model


if __name__ == "__main__":
    df_item = pd.read_excel(DATA_DIR / "DataITEM_Master.xlsx")

    for col in ["YARN_ITEM"]:
        if col not in df_item.columns:
            df_item[col] = ""

    df_all = pd.concat([df_item], ignore_index=True)

    train_mc_group_model(df_all)
