import gzip
import io
import json
import re
import csv
import sqlite3
from collections import defaultdict
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)
app.secret_key = "cyber_cell_secret_key_secure"

MEMORY_DB_URI = "file:case_memdb?mode=memory&cache=shared"
_db_anchor = sqlite3.connect(MEMORY_DB_URI, uri=True)

def get_db():
    conn = sqlite3.connect(MEMORY_DB_URI, uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(clear_all=False):
    with get_db() as conn:
        cursor = conn.cursor()
        if clear_all:
            cursor.execute("DROP TABLE IF EXISTS raw_csv_rows")
            cursor.execute("DROP TABLE IF EXISTS files_meta")
            cursor.execute("DROP TABLE IF EXISTS normalized_records")
            cursor.execute("DROP TABLE IF EXISTS node_tags")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_csv_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                row_json TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files_meta (
                filename TEXT PRIMARY KEY,
                headers_json TEXT,
                mode TEXT,
                source_col TEXT,
                target_col TEXT,
                dur_col TEXT,
                type_col TEXT,
                dt_col TEXT,
                time_col TEXT,
                linked_phone TEXT,
                target_identity TEXT,
                is_mapped INTEGER DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS normalized_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                mode TEXT,
                source TEXT,
                target TEXT,
                duration REAL,
                itype TEXT,
                idate TEXT,
                itime TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS node_tags (
                node_id TEXT PRIMARY KEY,
                tag TEXT
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_file ON raw_csv_rows(filename)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_norm_src ON normalized_records(source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_norm_tgt ON normalized_records(target)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_norm_file ON normalized_records(filename)")
        conn.commit()

init_db()

@app.after_request
def compress_response(response):
    try:
        accept_encoding = request.headers.get('Accept-Encoding', '')
        if 'gzip' not in accept_encoding.lower():
            return response
        if response.direct_passthrough or response.status_code >= 300:
            return response
        if len(response.get_data()) < 2048:
            return response

        compressed = gzip.compress(response.get_data(), compresslevel=6)
        response.set_data(compressed)
        response.headers['Content-Encoding'] = 'gzip'
        response.headers['Content-Length'] = len(compressed)
        return response
    except Exception:
        return response

MAX_LOGS_PER_NODE = 300
_NON_DIGIT_RE = re.compile(r'\D')

def clean_phone_str(val):
    if not val:
        return None
    digits = _NON_DIGIT_RE.sub('', str(val))
    if len(digits) == 12 and digits.startswith('91'):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith('0'):
        digits = digits[1:]
    return digits if len(digits) == 10 else None

def parse_float(val):
    if not val:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

COLUMN_ALIASES = {
    'source_col': ['target /a party number', 'a party number', 'a_party', 'source_acc', 'from_account', 'source', 'calling_number'],
    'target_col': ['b party number', 'b_party', 'target_acc', 'to_account', 'destination', 'called_number', 'target'],
    'dur_col': ['call duration', 'duration', 'amount', 'txn_amount', 'dur'],
    'type_col': ['call_type', 'service type', 'txn_type', 'method', 'type'],
    'dt_col': ['call date', 'date', 'txn_date', 'timestamp', 'datetime'],
    'time_col': ['call initiation time', 'call time', 'time', 'txn_time', 'time of call']
}

def auto_detect_columns(headers):
    cols_lower = {str(col).strip().lower(): str(col).strip() for col in headers if col}
    mapping = {}
    for standard_key, aliases in COLUMN_ALIASES.items():
        mapping[standard_key] = None
        for alias in aliases:
            if alias in cols_lower:
                mapping[standard_key] = cols_lower[alias]
                break
    return mapping

def _compute_graph_and_analysis():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM files_meta WHERE is_mapped = 1")
    mapped_files = cursor.fetchall()
    if not mapped_files:
        conn.close()
        return {
            'elements': [],
            'files': [],
            'meta': {'rendered_elements': 0, 'file_labels': {}},
            'analysis': {'targets': [], 'bridges': [], 'top_calls': [], 'top_durations': [], 'linked_assets': []}
        }

    file_labels = {f['filename']: (f['target_identity'] or f['filename']) for f in mapped_files}
    primary_targets = {f['target_identity'] for f in mapped_files if f['target_identity']}

    cursor.execute("SELECT filename, linked_phone, target_identity FROM files_meta WHERE mode = 'bank' AND linked_phone != 'none' AND is_mapped = 1")
    cross_links = cursor.fetchall()
    cross_edges = {(row['linked_phone'], row['target_identity']) for row in cross_links if row['linked_phone'] and row['target_identity']}
    phone_to_bank_map = defaultdict(set)
    for p, b in cross_edges:
        phone_to_bank_map[p].add(b)

    cursor.execute("SELECT node_id, tag FROM node_tags")
    node_tags = {str(row['node_id']): str(row['tag']) for row in cursor.fetchall() if row['node_id']}

    cursor.execute("""
        SELECT entity, SUM(cnt) AS total_count, COUNT(DISTINCT filename) AS file_count
        FROM (
            SELECT source AS entity, COUNT(*) AS cnt, filename FROM normalized_records WHERE source IS NOT NULL AND source != '' GROUP BY source, filename
            UNION ALL
            SELECT target AS entity, COUNT(*) AS cnt, filename FROM normalized_records WHERE target IS NOT NULL AND target != '' GROUP BY target, filename
        )
        GROUP BY entity
    """)
    node_stats = {str(row['entity']): {'count': int(row['total_count'] or 0), 'file_count': int(row['file_count'] or 0)} for row in cursor.fetchall() if row['entity']}

    cursor.execute("""
        SELECT 
            CASE WHEN source <= target THEN source ELSE target END AS pmin,
            CASE WHEN source <= target THEN target ELSE source END AS pmax,
            mode,
            COUNT(*) AS weight,
            COALESCE(SUM(duration), 0.0) AS dur_sum,
            MAX(CASE WHEN source <= target THEN 1 ELSE 0 END) AS has_fwd,
            MAX(CASE WHEN source > target THEN 1 ELSE 0 END) AS has_bwd
        FROM normalized_records
        WHERE source IS NOT NULL AND target IS NOT NULL AND source != '' AND target != ''
        GROUP BY pmin, pmax, mode
    """)
    raw_edges = cursor.fetchall()

    neighbor_sets = defaultdict(set)
    for row in raw_edges:
        pmin, pmax = str(row['pmin']), str(row['pmax'])
        neighbor_sets[pmin].add(pmax)
        neighbor_sets[pmax].add(pmin)

    top_volume_nodes = set()
    top_duration_nodes = set()
    node_reasons = defaultdict(list)
    analysis_top_calls = []
    analysis_top_durations = []

    for f in mapped_files:
        fname = f['filename']
        f_target = str(f['target_identity'] or '')

        cursor.execute("""
            SELECT entity, SUM(cnt) AS total_calls
            FROM (
                SELECT source AS entity, COUNT(*) AS cnt FROM normalized_records WHERE filename = ? AND source != '' GROUP BY source
                UNION ALL
                SELECT target AS entity, COUNT(*) AS cnt FROM normalized_records WHERE filename = ? AND target != '' GROUP BY target
            )
            WHERE entity != ?
            GROUP BY entity
            ORDER BY total_calls DESC
            LIMIT 3
        """, (fname, fname, f_target))
        for row in cursor.fetchall():
            ent = str(row['entity'])
            cnt = int(row['total_calls'] or 0)
            top_volume_nodes.add(ent)
            node_reasons[ent].append(f"Top 3 Calls in {fname} ({cnt} calls)")
            analysis_top_calls.append({
                'file': fname,
                'target': f_target,
                'contact': ent,
                'calls': cnt,
                'tag': node_tags.get(ent, '')
            })

        cursor.execute("""
            SELECT 
                CASE WHEN source = ? THEN target ELSE source END AS contact,
                COALESCE(SUM(duration), 0.0) AS total_dur
            FROM normalized_records
            WHERE filename = ? AND mode = 'phone' AND (source = ? OR target = ?)
            GROUP BY contact
            ORDER BY total_dur DESC
            LIMIT 5
        """, (f_target, fname, f_target, f_target))
        for row in cursor.fetchall():
            contact = str(row['contact'])
            dur = float(row['total_dur'] or 0.0)
            if dur > 0 and contact != f_target:
                top_duration_nodes.add(contact)
                node_reasons[contact].append(f"Top 5 Duration in {fname} ({int(dur)}s)")
                analysis_top_durations.append({
                    'file': fname,
                    'target': f_target,
                    'contact': contact,
                    'duration_sec': int(dur),
                    'tag': node_tags.get(contact, '')
                })

    critical_nodes = set(primary_targets) | set(top_volume_nodes) | set(top_duration_nodes)

    for pt in primary_targets:
        if pt:
            node_reasons[pt].append("Target Suspect")

    for p, b in cross_edges:
        critical_nodes.add(p)
        critical_nodes.add(b)
        node_reasons[p].append("Linked Bank Asset")
        node_reasons[b].append("Linked Phone Asset")

    for node in node_tags:
        critical_nodes.add(node)
        node_reasons[node].append("Investigator Tagged")

    analysis_bridges = []
    for entity, stats in node_stats.items():
        if stats['file_count'] >= 2:
            critical_nodes.add(entity)
            node_reasons[entity].append(f"Common across {stats['file_count']} CDR files")
            analysis_bridges.append({
                'entity': entity,
                'file_count': stats['file_count'],
                'total_interactions': stats['count'],
                'tag': node_tags.get(entity, '')
            })

    elements = []
    routine_buckets = defaultdict(lambda: {'members': [], 'total_interactions': 0})

    for entity, stats in node_stats.items():
        is_phone = (len(entity) == 10 and entity.isdigit())
        node_type = 'phone' if is_phone else 'bank'
        degree = len(neighbor_sets.get(entity, ()))

        if entity in critical_nodes:
            size = min(22 + (degree * 4), 65) if is_phone else min(22 + (degree * 5), 70)
            is_linked = entity in phone_to_bank_map

            if entity in primary_targets:
                role_class = "role_main_target"
            elif stats['file_count'] >= 2:
                role_class = "role_bridge_node"
            elif is_linked:
                role_class = "phone_orange"
            elif is_phone:
                role_class = "phone_standard"
            else:
                role_class = "bank_node"

            ring_class = ""
            is_top_v = entity in top_volume_nodes
            is_top_d = entity in top_duration_nodes
            if is_top_v and is_top_d:
                ring_class = "ring_top_both"
            elif is_top_v:
                ring_class = "ring_top_volume"
            elif is_top_d:
                ring_class = "ring_top_duration"

            classes = f"{role_class} {ring_class}".strip()

            elements.append({
                'data': {
                    'id': entity,
                    'phone_number' if is_phone else 'account_number': entity,
                    'total_interactions': stats['count'],
                    'connections': degree,
                    'size': size,
                    'node_type': node_type,
                    'linked_banks': list(phone_to_bank_map[entity]) if is_linked else [],
                    'tag': node_tags.get(entity, ''),
                    'flags': list(set(node_reasons.get(entity, [])))
                },
                'classes': classes
            })
        else:
            neighbors = neighbor_sets.get(entity, set())
            if neighbors:
                hub = max(neighbors, key=lambda n: node_stats.get(n, {}).get('count', 0))
                routine_buckets[(hub, node_type)]['members'].append(entity)
                routine_buckets[(hub, node_type)]['total_interactions'] += stats['count']

    rendered_node_ids = {el['data']['id'] for el in elements}

    for (hub, ntype), bucket in routine_buckets.items():
        if hub not in rendered_node_ids:
            continue
        gid = f"group_{hub}_{ntype}"
        count = len(bucket['members'])
        elements.append({
            'data': {
                'id': gid,
                'node_type': 'group',
                'member_type': ntype,
                'group_count': count,
                'member_ids': bucket['members'],
                'total_interactions': bucket['total_interactions'],
                'size': min(22 + (count * 2), 65),
                'group_label': f"{count} Routine {'Calls' if ntype == 'phone' else 'Accounts'}"
            },
            'classes': 'group_node'
        })
        elements.append({
            'data': {
                'id': f"{gid}_edge",
                'source': hub,
                'target': gid,
                'weight': f"{count} contacts",
                'numeric_weight': bucket['total_interactions'],
                'arrow_mode': 'none',
                'edge_type': 'group_link',
                'is_top_3': False
            },
            'classes': 'group_link_edge'
        })

    for i, row in enumerate(raw_edges):
        u, v = str(row['pmin']), str(row['pmax'])
        if u in rendered_node_ids and v in rendered_node_ids:
            has_fwd, has_bwd = bool(row['has_fwd']), bool(row['has_bwd'])
            arrow_mode = "both" if (has_fwd and has_bwd) else ("forward" if has_fwd else "backward")
            display_label = f"₹{row['dur_sum']} ({row['weight']}tx)" if row['mode'] == 'bank' else f"{row['weight']} calls"

            elements.append({
                'data': {
                    'id': f"edge_{i}",
                    'source': u,
                    'target': v,
                    'weight': display_label,
                    'numeric_weight': int(row['weight']),
                    'arrow_mode': arrow_mode,
                    'edge_type': 'financial' if row['mode'] == 'bank' else 'telecom',
                    'is_top_3': False
                },
                'classes': 'financial_edge' if row['mode'] == 'bank' else 'telecom_edge'
            })

    for idx, (p, b) in enumerate(cross_edges):
        if p in rendered_node_ids and b in rendered_node_ids:
            elements.append({
                'data': {
                    'id': f"cross_tether_{idx}",
                    'source': p,
                    'target': b,
                    'weight': 'LINKED ASSET',
                    'arrow_mode': 'none',
                    'edge_type': 'cross_link'
                },
                'classes': 'cross_link_edge'
            })

    conn.close()
    return {
        'elements': elements,
        'files': [f['filename'] for f in mapped_files],
        'meta': {
            'rendered_elements': len(elements),
            'total_nodes': len(rendered_node_ids),
            'file_labels': file_labels
        },
        'analysis': {
            'targets': [
                {'filename': f['filename'], 'target': f['target_identity'] or 'Unidentified', 'mode': f['mode'] or 'phone', 'tag': node_tags.get(f['target_identity'], '')}
                for f in mapped_files
            ],
            'bridges': analysis_bridges,
            'top_calls': analysis_top_calls,
            'top_durations': analysis_top_durations,
            'linked_assets': [
                {'phone': p, 'bank': b, 'phone_tag': node_tags.get(p, ''), 'bank_tag': node_tags.get(b, '')}
                for p, b in cross_edges
            ]
        }
    }

def _graph_response():
    data = _compute_graph_and_analysis()
    return jsonify({
        'files': data['files'],
        'graph': data['elements'],
        'graph_meta': data['meta'],
        'analysis': data['analysis']
    })

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/clear-session', methods=['POST'])
def clear_session():
    init_db(clear_all=True)
    return jsonify({'status': 'cleared'})

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files' not in request.files:
        return jsonify({'error': 'No file element detected.'}), 400

    uploaded_files = request.files.getlist('files')
    if not uploaded_files or uploaded_files[0].filename == '':
        return jsonify({'error': 'No document selected.'}), 400

    filenames = []
    auto_mappings = {}
    primary_headers = []

    conn = get_db()
    cursor = conn.cursor()

    try:
        for idx, file in enumerate(uploaded_files):
            if file and file.filename.endswith('.csv'):
                filename = file.filename
                filenames.append(filename)

                cursor.execute("DELETE FROM raw_csv_rows WHERE filename = ?", (filename,))
                cursor.execute("DELETE FROM files_meta WHERE filename = ?", (filename,))
                cursor.execute("DELETE FROM normalized_records WHERE filename = ?", (filename,))

                stream = io.StringIO(file.read().decode("utf-8", errors="ignore"))
                reader = csv.reader(stream)
                headers = next(reader, None)

                if not headers:
                    continue

                headers = [str(h).strip() for h in headers if h]
                cursor.execute("""
                    INSERT INTO files_meta (filename, headers_json, is_mapped)
                    VALUES (?, ?, 0)
                """, (filename, json.dumps(headers)))

                if idx == 0:
                    primary_headers = headers
                    auto_mappings = auto_detect_columns(headers)

                insert_batch = []
                for row in reader:
                    if not row or not any(row):
                        continue
                    row_dict = {headers[i]: str(row[i]).strip() for i in range(min(len(headers), len(row)))}
                    insert_batch.append((filename, json.dumps(row_dict)))

                    if len(insert_batch) >= 5000:
                        cursor.executemany("INSERT INTO raw_csv_rows (filename, row_json) VALUES (?, ?)", insert_batch)
                        insert_batch = []

                if insert_batch:
                    cursor.executemany("INSERT INTO raw_csv_rows (filename, row_json) VALUES (?, ?)", insert_batch)

        conn.commit()

        cursor.execute("SELECT DISTINCT source FROM normalized_records WHERE mode = 'phone' UNION SELECT DISTINCT target FROM normalized_records WHERE mode = 'phone'")
        existing_phones = [str(r[0]) for r in cursor.fetchall() if r[0] and len(str(r[0])) == 10]
        conn.close()

        return jsonify({
            'filenames': filenames,
            'headers': primary_headers,
            'auto_mapping': auto_mappings,
            'existing_phones': existing_phones
        })
    except Exception as e:
        conn.rollback()
        conn.close()
        return jsonify({'error': f'Failed reading CSV stream: {str(e)}'}), 500

def _normalize_and_map_file(conn, filename, mode, src_col, tgt_col, dur_col, type_col, dt_col, time_col, linked_phone):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM normalized_records WHERE filename = ?", (filename,))
    cursor.execute("SELECT row_json FROM raw_csv_rows WHERE filename = ?", (filename,))

    norm_batch = []
    counts = defaultdict(int)
    first_src = None

    while True:
        rows = cursor.fetchmany(5000)
        if not rows:
            break

        for r in rows:
            row_data = json.loads(r['row_json'])
            raw_s = (row_data.get(src_col) or '').strip()
            raw_t = (row_data.get(tgt_col) or '').strip()

            if not raw_s or not raw_t:
                continue

            if mode == 'phone':
                u = clean_phone_str(raw_s)
                v = clean_phone_str(raw_t)
                if not u or not v:
                    continue
            else:
                u, v = raw_s, raw_t
                if first_src is None:
                    first_src = u

            counts[u] += 1
            counts[v] += 1
            dur_val = parse_float(row_data.get(dur_col)) if dur_col else 0.0

            norm_batch.append((
                filename, mode, u, v, dur_val,
                row_data.get(type_col, '') if type_col else '',
                row_data.get(dt_col, '') if dt_col else '',
                row_data.get(time_col, '') if time_col else ''
            ))

        if len(norm_batch) >= 5000:
            cursor.executemany("""
                INSERT INTO normalized_records (filename, mode, source, target, duration, itype, idate, itime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, norm_batch)
            norm_batch = []

    if norm_batch:
        cursor.executemany("""
            INSERT INTO normalized_records (filename, mode, source, target, duration, itype, idate, itime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, norm_batch)

    target_identity = max(counts, key=counts.get) if (mode == 'phone' and counts) else (first_src or '')

    cursor.execute("""
        UPDATE files_meta 
        SET mode = ?, source_col = ?, target_col = ?, dur_col = ?, type_col = ?, 
            dt_col = ?, time_col = ?, linked_phone = ?, target_identity = ?, is_mapped = 1
        WHERE filename = ?
    """, (mode, src_col, tgt_col, dur_col, type_col, dt_col, time_col, linked_phone, target_identity, filename))

@app.route('/process-batch-mapping', methods=['POST'])
def process_batch_mapping():
    data = request.json or {}
    filenames = data.get('filenames', [])
    mode = data.get('mode', 'phone')
    src_col = data.get('source_col')
    tgt_col = data.get('target_col')
    dur_col = data.get('dur_col')
    type_col = data.get('type_col')
    dt_col = data.get('dt_col')
    time_col = data.get('time_col')
    linked_phone = data.get('linked_phone', 'none')

    conn = get_db()
    try:
        for filename in filenames:
            _normalize_and_map_file(conn, filename, mode, src_col, tgt_col, dur_col, type_col, dt_col, time_col, linked_phone)
        conn.commit()
        conn.close()
        return _graph_response()
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        return jsonify({'error': f'Batch processing faulted: {str(e)}'}), 500

@app.route('/expand-member/<path:node_id>', methods=['GET'])
def expand_member(node_id):
    node_type = request.args.get('type', 'phone')
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) as total_cnt
        FROM normalized_records
        WHERE source = ? OR target = ?
    """, (node_id, node_id))
    total_interactions = int(cursor.fetchone()['total_cnt'] or 0)

    cursor.execute("""
        SELECT tag FROM node_tags WHERE node_id = ?
    """, (node_id,))
    tag_row = cursor.fetchone()
    tag_val = tag_row['tag'] if tag_row else ''

    cursor.execute("""
        SELECT 
            CASE WHEN source <= target THEN source ELSE target END AS pmin,
            CASE WHEN source <= target THEN target ELSE source END AS pmax,
            mode,
            COUNT(*) AS weight,
            COALESCE(SUM(duration), 0.0) AS dur_sum,
            MAX(CASE WHEN source <= target THEN 1 ELSE 0 END) AS has_fwd,
            MAX(CASE WHEN source > target THEN 1 ELSE 0 END) AS has_bwd
        FROM normalized_records
        WHERE source = ? OR target = ?
        GROUP BY pmin, pmax, mode
    """, (node_id, node_id))
    raw_edges = cursor.fetchall()
    conn.close()

    degree = len(raw_edges)
    is_phone = (node_type == 'phone')
    size = min(22 + (degree * 4), 65) if is_phone else min(22 + (degree * 5), 70)
    base_class = "phone_standard" if is_phone else "bank_node"

    node_element = {
        'data': {
            'id': node_id,
            'phone_number' if is_phone else 'account_number': node_id,
            'total_interactions': total_interactions,
            'connections': degree,
            'size': size,
            'node_type': node_type,
            'linked_banks': [],
            'tag': tag_val,
            'flags': ['Unpacked from Routine Bucket']
        },
        'classes': f"{base_class}"
    }

    edge_elements = []
    for i, row in enumerate(raw_edges):
        u, v = str(row['pmin']), str(row['pmax'])
        has_fwd, has_bwd = bool(row['has_fwd']), bool(row['has_bwd'])
        arrow_mode = "both" if (has_fwd and has_bwd) else ("forward" if has_fwd else "backward")
        display_label = f"₹{row['dur_sum']} ({row['weight']}tx)" if row['mode'] == 'bank' else f"{row['weight']} calls"

        edge_elements.append({
            'data': {
                'id': f"expand_{node_id}_edge_{i}",
                'source': u,
                'target': v,
                'weight': display_label,
                'numeric_weight': int(row['weight']),
                'arrow_mode': arrow_mode,
                'edge_type': 'financial' if row['mode'] == 'bank' else 'telecom',
                'is_top_3': False
            },
            'classes': 'financial_edge' if row['mode'] == 'bank' else 'telecom_edge'
        })

    return jsonify({'node': node_element, 'edges': edge_elements})

@app.route('/node-details/<path:node_id>', methods=['GET'])
def node_details(node_id):
    node_type = request.args.get('type', 'phone')
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT source, target, duration, itype, idate, itime 
        FROM normalized_records 
        WHERE source = ? OR target = ? 
        LIMIT ?
    """, (node_id, node_id, MAX_LOGS_PER_NODE))
    
    rows = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) FROM normalized_records WHERE source = ? OR target = ?", (node_id, node_id))
    total = int(cursor.fetchone()[0] or 0)
    conn.close()

    records = []
    for r in rows:
        rec = {
            'from': r['source'],
            'to': r['target'],
            'date': r['idate'],
            'time': r['itime']
        }
        if node_type == 'phone':
            rec['type'] = r['itype']
            rec['duration'] = r['duration']
        else:
            rec['amount'] = r['duration']
        records.append(rec)

    return jsonify({'records': records, 'total': total})

@app.route('/tag/<path:node_id>', methods=['POST'])
def set_tag(node_id):
    data = request.json or {}
    tag = (data.get('tag') or '').strip()
    conn = get_db()
    cursor = conn.cursor()
    if tag:
        cursor.execute("INSERT OR REPLACE INTO node_tags (node_id, tag) VALUES (?, ?)", (node_id, tag))
    else:
        cursor.execute("DELETE FROM node_tags WHERE node_id = ?", (node_id,))
    conn.commit()
    conn.close()
    return jsonify({'node_id': node_id, 'tag': tag})

@app.route('/get-current-state', methods=['GET'])
def get_current_state():
    return _graph_response()

@app.route('/delete/<path:filename>', methods=['DELETE'])
def delete_file(filename):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM raw_csv_rows WHERE filename = ?", (filename,))
    cursor.execute("DELETE FROM normalized_records WHERE filename = ?", (filename,))
    cursor.execute("DELETE FROM files_meta WHERE filename = ?", (filename,))
    conn.commit()
    conn.close()
    return _graph_response()

@app.route('/export-workspace', methods=['GET'])
def export_workspace():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM files_meta WHERE is_mapped = 1")
    meta_rows = [dict(row) for row in cursor.fetchall()]

    cursor.execute("SELECT * FROM raw_csv_rows")
    raw_rows = [dict(row) for row in cursor.fetchall()]

    cursor.execute("SELECT * FROM node_tags")
    tags = {str(row['node_id']): str(row['tag']) for row in cursor.fetchall()}
    conn.close()

    bundle = {
        'version': 4,
        'files_meta': meta_rows,
        'raw_rows': raw_rows,
        'node_tags': tags
    }
    return jsonify(bundle)

@app.route('/import-workspace', methods=['POST'])
def import_workspace():
    if 'workspace_file' not in request.files:
        return jsonify({'error': 'No workspace file provided.'}), 400

    try:
        raw = request.files['workspace_file'].read().decode('utf-8', errors='ignore')
        bundle = json.loads(raw)

        init_db(clear_all=True)
        conn = get_db()
        cursor = conn.cursor()

        for meta in bundle.get('files_meta', []):
            cursor.execute("""
                INSERT INTO files_meta (filename, headers_json, mode, source_col, target_col, dur_col, type_col, dt_col, time_col, linked_phone, target_identity, is_mapped)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                meta['filename'], meta['headers_json'], meta['mode'], meta['source_col'], meta['target_col'],
                meta['dur_col'], meta['type_col'], meta['dt_col'], meta['time_col'], meta['linked_phone'],
                meta['target_identity'], meta['is_mapped']
            ))

        raw_batch = [(r['filename'], r['row_json']) for r in bundle.get('raw_rows', [])]
        if raw_batch:
            cursor.executemany("INSERT INTO raw_csv_rows (filename, row_json) VALUES (?, ?)", raw_batch)

        for nid, tag in bundle.get('node_tags', {}).items():
            cursor.execute("INSERT INTO node_tags (node_id, tag) VALUES (?, ?)", (nid, tag))

        for meta in bundle.get('files_meta', []):
            if meta.get('is_mapped'):
                _normalize_and_map_file(
                    conn,
                    meta['filename'],
                    meta.get('mode', 'phone'),
                    meta.get('source_col'),
                    meta.get('target_col'),
                    meta.get('dur_col'),
                    meta.get('type_col'),
                    meta.get('dt_col'),
                    meta.get('time_col'),
                    meta.get('linked_phone', 'none')
                )

        conn.commit()
        conn.close()
        return _graph_response()
    except Exception as e:
        return jsonify({'error': f'Failed to restore workspace: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True, port=80)