"""
merge_characters.py - Merge duplicate wiki_characters into canonical entries.

Review MERGES list before running.
Run: uv run python3 merge_characters.py
"""

import json
import sqlite3

DB_PATH = "data/mao-son-troc-quy-nhan.db"

# (keep_id, delete_id)
# Aliases are auto-merged: delete's name + aliases_json are absorbed into keep.
MERGES = [
    # Nhue Lanh Ngoc = Vo Cuc Quy Vuong / Lanh Ngoc
    ("nhue_lanh_ngoc",       "vo_cuc_quy_vuong"),
    ("nhue_lanh_ngoc",       "vo_quy_vuong"),
    ("nhue_lanh_ngoc",       "lanh_ngoc"),

    # Ta Vu Tinh = Ta Vu Ninh
    ("ta_vu_tinh",           "ta_vu_ninh"),

    # Tu Bao = Tu Bao phap su / Ngo Gia Dao
    ("tu_bao",               "tu_bao_phap_su"),
    ("tu_bao",               "ngo_gia_dao"),
    ("tu_bao",               "tu_bao_pha_su"),

    # A Ngoc = Hau Khanh
    ("a_ngoc",               "hau_khanh"),

    # Mo Han = Die Th Vuong
    ("mo_han",               "die_th_vuong"),

    # Thong Huyen dao nhan = Thong Huyen
    ("thong_huyen_dao_nhan", "thong_huyen"),

    # Tri Tham Thien Su = Tiep Dan Dao Nhan
    ("tri_tham_thien_su",    "tiep_dan_dao_nhan"),

    # Trieu Le Du = Tieu Ngu -- 3 ID duplicate
    ("trieu_le_du",          "xia_ngu"),
    ("trieu_le_du",          "xia_nuer_co"),
    ("trieu_le_du",          "zhi_lian_du"),

    # Thanh Van Tu = Lao Dao Si
    ("thanh_van_tu",         "lao_dao_si"),

    # Dao Phong -- dao_phong co 194 snaps, duong_phong co 0
    ("dao_phong",            "duong_phong"),

    # Trieu Cong Minh -- typo ID triau_cong_minh
    ("trieu_cong_minh",      "triau_cong_minh"),

    # Tieu Ma = Dai Luc Quy Vuong
    ("tieu_ma",              "dia_luc_quy_vuong"),

    # Chu Tinh Nhu = Chu Tinh Ngu
    ("chu_tinh_nhu",         "chu_tinh_ngu"),

    # Cung Tu = Duong Cung Tu
    ("duong_cung_tu",              "cung_tu"),
    
    # Thôi Phủ Quân
    ("thoi_phu_quan",        "thoi_ngoc"),
    ("thoi_phu_quan",        "thoi_sinh"),
    
    # Chung Quỳ
    ("chung_quy",            "chung_quy_thien_su"),
    
    # Đàm Vũ Tuệ
    ("dam_vu_tue",           "dai_vu_tien"),
    
    # Kiến Văn Đế Chu Lệ
    ("chu_le",                "kien_van_de"),
    ("chu_le",                "kien_van_de_thanh_nguoi_khac"),

    # Giao nhan = Dong Hai Giao Nhan (same aliases "My nhan ngu", chap 397 vs 416)
    ("dong_hai_gao_nhan",    "giao_nhan"),

    # Vo Cuc thien su -- typo "Cuoc" vs "Cuc"
    ("vo_cuc_thien_su",      "vo_cuoc_thien_su"),

    # Tieu Thanh Tieu Bach -- combined ID, merge into Tieu Thanh
    ("tieu_thanh",           "tieu_thanh_tieu_bach"),
    
    # Diep Thieu Duong = Thieu Duong -- 2 ID duplicate
    ("diep_thieu_duong",      "thieu_duong"),
]


def merge_character(con: sqlite3.Connection, keep: str, delete: str) -> None:
    # Move snapshots
    con.execute(
        "UPDATE wiki_snapshots SET character_id=? WHERE character_id=?",
        (keep, delete),
    )
    # Move relations
    con.execute(
        "UPDATE wiki_relations SET character_id=? WHERE character_id=?",
        (keep, delete),
    )
    # Move artifact ownership
    con.execute(
        "UPDATE wiki_artifact_snapshots SET owner_id=? WHERE owner_id=?",
        (keep, delete),
    )
    # Move mention index (ignore duplicates)
    con.execute(
        """INSERT OR IGNORE INTO wiki_mention_index (character_id, chapter_num)
           SELECT ?, chapter_num FROM wiki_mention_index WHERE character_id=?""",
        (keep, delete),
    )
    con.execute(
        "DELETE FROM wiki_mention_index WHERE character_id=?",
        (delete,),
    )
    # Move char_batches
    try:
        con.execute(
            "UPDATE wiki_char_batches SET character_id=? WHERE character_id=?",
            (keep, delete),
        )
    except sqlite3.OperationalError as e:
        print(f"  [WARN] wiki_char_batches update skipped: {e}")

    # Merge aliases: keep's aliases + delete's name + delete's aliases
    keep_row = con.execute(
        "SELECT name, aliases_json FROM wiki_characters WHERE character_id=?", (keep,)
    ).fetchone()
    delete_row = con.execute(
        "SELECT name, aliases_json FROM wiki_characters WHERE character_id=?", (delete,)
    ).fetchone()
    if not keep_row:
        return  # keep was already deleted or missing
    keep_name = keep_row[0]
    # Deduplicate initial aliases from DB
    raw_aliases: list[str] = json.loads(keep_row[1]) if keep_row[1] else []
    seen: set[str] = {keep_name}
    aliases: list[str] = []
    for a in raw_aliases:
        if a and a not in seen:
            aliases.append(a)
            seen.add(a)
    # Absorb delete's name and all its aliases
    if delete_row:
        candidates = [delete_row[0]] + (json.loads(delete_row[1]) if delete_row[1] else [])
        for candidate in candidates:
            if candidate and candidate not in seen:
                aliases.append(candidate)
                seen.add(candidate)
    con.execute(
        "UPDATE wiki_characters SET aliases_json=? WHERE character_id=?",
        (json.dumps(aliases, ensure_ascii=False), keep),
    )

    # Delete duplicate
    con.execute("DELETE FROM wiki_characters WHERE character_id=?", (delete,))


def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = OFF")

    for keep, delete in MERGES:
        keep_row = con.execute(
            "SELECT name FROM wiki_characters WHERE character_id=?", (keep,)
        ).fetchone()
        delete_row = con.execute(
            "SELECT name FROM wiki_characters WHERE character_id=?", (delete,)
        ).fetchone()

        if not keep_row:
            print(f"[SKIP] keep '{keep}' not found in DB")
            continue
        if not delete_row:
            print(f"[SKIP] delete '{delete}' not found in DB (already merged?)")
            continue

        print(f"Merging: {delete} ({delete_row[0]}) -> {keep} ({keep_row[0]})")
        merge_character(con, keep, delete)
        print(f"  Done.")

    con.commit()
    con.close()
    print("\nAll merges committed.")


if __name__ == "__main__":
    main()
