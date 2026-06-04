import sqlite3, json

DB_PATH = 'data/mao-son-troc-quy-nhan.db'

MERGES = [
    ('nhue_lanh_ngoc','vo_cuc_quy_vuong'),('nhue_lanh_ngoc','vo_quy_vuong'),('nhue_lanh_ngoc','lanh_ngoc'),
    ('ta_vu_tinh','ta_vu_ninh'),
    ('tu_bao','tu_bao_phap_su'),('tu_bao','ngo_gia_dao'),('tu_bao','tu_bao_pha_su'),
    ('a_ngoc','hau_khanh'),('mo_han','die_th_vuong'),
    ('thong_huyen_dao_nhan','thong_huyen'),('tri_tham_thien_su','tiep_dan_dao_nhan'),
    ('trieu_le_du','xia_ngu'),('trieu_le_du','xia_nuer_co'),('trieu_le_du','zhi_lian_du'),
    ('thanh_van_tu','lao_dao_si'),('dao_phong','duong_phong'),
    ('trieu_cong_minh','triau_cong_minh'),
    ('tieu_ma','dia_luc_quy_vuong'),('tieu_ma','dai_luc_quy_vuong'),
    ('chu_tinh_nhu','chu_tinh_ngu'),('cung_tu','duong_cung_tu'),
]

keep_to_deletes = {}
delete_to_keep = {}
for keep, delete in MERGES:
    keep_to_deletes.setdefault(keep, []).append(delete)
    delete_to_keep[delete] = keep

con = sqlite3.connect(DB_PATH)
rows = con.execute('''
    SELECT wc.character_id, wc.name, wc.aliases_json, COUNT(wmi.id) AS mentions
    FROM wiki_characters wc
    LEFT JOIN wiki_mention_index wmi ON wmi.character_id = wc.character_id
    WHERE EXISTS (SELECT 1 FROM wiki_snapshots ws WHERE ws.character_id = wc.character_id AND ws.is_active = 1)
    GROUP BY wc.character_id
    ORDER BY mentions DESC, wc.name COLLATE NOCASE
''').fetchall()
con.close()

lines = ['| # | character_id | name | aliases | mentions | merge |', '|---|---|---|---|---|---|']
for i, (cid, name, aj, mentions) in enumerate(rows, 1):
    try:
        aliases = ', '.join(json.loads(aj)) if aj else ''
    except Exception:
        aliases = aj or ''
    if cid in delete_to_keep:
        merge_note = 'del -> ' + delete_to_keep[cid]
    elif cid in keep_to_deletes:
        merge_note = 'keep <- ' + ', '.join(keep_to_deletes[cid])
    else:
        merge_note = ''
    lines.append(f'| {i} | {cid} | {name} | {aliases} | {mentions} | {merge_note} |')

with open('data/characters_list.md', 'w', encoding='utf-8') as f:
    f.write('# Danh sach nhan vat (active)\n\n' + '\n'.join(lines) + '\n')
print(f'Done -- {len(rows)} characters')
