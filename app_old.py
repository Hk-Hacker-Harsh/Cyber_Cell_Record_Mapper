import gzip
import io
import json
import re
from collections import defaultdict

import numpy as np
import pandas as pd
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)
app.secret_key = "cyber_cell_secret_key_secure"


@app.after_request
def compress_response(response):
    """Gzip-compress JSON responses. Graph payloads for large multi-file
    batches are mostly repeated field names/short strings, which gzip
    shrinks dramatically (often 5-10x) - this is the biggest lever for
    'file too big to load' symptoms since it cuts network transfer time."""
    accept_encoding = request.headers.get('Accept-Encoding', '')
    if 'gzip' not in accept_encoding.lower():
        return response
    if response.direct_passthrough or response.status_code >= 300:
        return response
    if len(response.get_data()) < 2048:  # not worth compressing tiny responses
        return response

    compressed = gzip.compress(response.get_data(), compresslevel=6)
    response.set_data(compressed)
    response.headers['Content-Encoding'] = 'gzip'
    response.headers['Content-Length'] = len(compressed)
    return response

# Volatile Data-Cache structures, isolated per process-runtime lifecycle
app.file_configs = {}
app.global_active_files = []
app.cleaned_dfs = {}

# Graph-wide "collapse low-activity contacts" setting. Global (not per-file)
# because node identity spans across files - the same number/account can
# appear in multiple staged documents.
app.grouping_config = {'enabled': False, 'metric': 'count', 'threshold': 0}

# Investigator-assigned names/tags for numbers & accounts (e.g. "Suspect A",
# "Mule Account 3"). Keyed by node id, global across the case.
app.node_tags = {}

# Friendly per-file labels (e.g. the primary/most-frequent number in that
# file) computed automatically after mapping, so the sidebar doesn't just
# show raw uploaded filenames.
app.file_display_names = {}

# Cap on how many raw records we attach to a single node's detail panel.
# Keeps the JSON payload (and browser) fast even on very large CDR dumps.
MAX_LOGS_PER_NODE = 300

# Hard ceiling on rendered elements before we warn the investigator the
# graph is still too dense (browser physics layout is what actually
# crashes/freezes on very large graphs, independent of payload size).
RENDER_WARNING_THRESHOLD = 3000

_NON_DIGIT_RE = re.compile(r'\D')


def normalize_phone_series(series):
    """
    Data-cleaning step: strips everything to digits, removes a leading
    country code (91) or trunk prefix (0) if present, and keeps only values
    that resolve to a clean 10-digit mobile number. Anything else becomes
    NaN so the normal dropna() validity filter removes that row.
    """
    digits = series.astype(str).str.replace(_NON_DIGIT_RE, '', regex=True)
    is_12_with_91 = (digits.str.len() == 12) & digits.str.startswith('91')
    digits = digits.where(~is_12_with_91, digits.str.slice(2))
    is_11_with_0 = (digits.str.len() == 11) & digits.str.startswith('0')
    digits = digits.where(~is_11_with_0, digits.str.slice(1))
    valid = digits.str.len() == 10
    return digits.where(valid, np.nan)


def _valid_phone_frame(df, src_col, tgt_col):
    """Returns the subset of df with src/tgt normalized to clean 10-digit
    numbers, with unparsable/invalid-length rows dropped entirely."""
    sub = df.dropna(subset=[src_col, tgt_col]).copy()
    sub[src_col] = normalize_phone_series(sub[src_col])
    sub[tgt_col] = normalize_phone_series(sub[tgt_col])
    return sub.dropna(subset=[src_col, tgt_col])



# Standard column alias mapper for automatic matching
COLUMN_ALIASES = {
    'source_col': ['target /a party number', 'a party number', 'a_party', 'source_acc', 'from_account', 'source', 'calling_number'],
    'target_col': ['b party number', 'b_party', 'target_acc', 'to_account', 'destination', 'called_number', 'target'],
    'dur_col': ['call duration', 'duration', 'amount', 'txn_amount', 'dur'],
    'type_col': ['call_type', 'service type', 'txn_type', 'method', 'type'],
    'dt_col': ['call date', 'date', 'txn_date', 'timestamp', 'datetime'],
    'time_col': ['call initiation time', 'call time', 'time', 'txn_time', 'time of call']
}


def auto_detect_columns(df_columns):
    """Automatically maps raw CSV column headers to internal standard keys."""
    cols_lower = {col.strip().lower(): col.strip() for col in df_columns}
    mapping = {}
    for standard_key, aliases in COLUMN_ALIASES.items():
        mapping[standard_key] = None
        for alias in aliases:
            if alias in cols_lower:
                mapping[standard_key] = cols_lower[alias]
                break
    return mapping


def clean_dataframe(df):
    """Initial Data Cleaning: strips strings, removes NaN/Null equivalents, normalizes identifiers."""
    df.columns = df.columns.str.strip()

    # Strip whitespace & normalize NaN variations across all text columns (vectorized, no per-row apply)
    for col in df.columns:
        s = df[col].astype(str).str.strip()
        df[col] = s.replace(['nan', 'NaN', 'NAN', 'None', 'null', 'NULL', 'N/A', '-'], np.nan)

    return df


def _safe_col(df, col_name, fallback=''):
    """Returns a Series for col_name if it exists & is usable, else a constant fallback Series."""
    if col_name and col_name in df.columns:
        return df[col_name].fillna('')
    return pd.Series([fallback] * len(df), index=df.index)


def _aggregate_pairs(valid_df, src_col, tgt_col, label_col, edge_type, raw_edges):
    """
    Vectorized replacement for the old per-row `iterrows()` edge builder.
    Computes, for every unique unordered (node_a, node_b) pair:
      - total interaction weight
      - whether a->b and/or b->a directions occurred
      - a representative label (first non-null amount/duration seen)
    then merges the (small) per-pair result into the shared raw_edges dict.
    """
    if valid_df.empty:
        return

    s = valid_df[src_col].to_numpy()
    t = valid_df[tgt_col].to_numpy()

    pmin = np.where(s <= t, s, t)
    pmax = np.where(s <= t, t, s)
    is_forward = (s == pmin)

    pair_df = pd.DataFrame({'pmin': pmin, 'pmax': pmax, 'is_forward': is_forward})
    if label_col and label_col in valid_df.columns:
        pair_df['label'] = valid_df[label_col].to_numpy()
        pair_df['numeric_val'] = pd.to_numeric(valid_df[label_col], errors='coerce').fillna(0).to_numpy()
    else:
        pair_df['label'] = None
        pair_df['numeric_val'] = 0.0

    grouped = pair_df.groupby(['pmin', 'pmax'], sort=False)
    agg = grouped.agg(
        weight=('is_forward', 'size'),
        has_fwd=('is_forward', 'any'),
        has_bwd=('is_forward', lambda x: bool((~x).any())),
        label=('label', lambda x: next((v for v in x if pd.notna(v)), None)),
        value_sum=('numeric_val', 'sum'),
    ).reset_index()

    # itertuples (no per-row Series construction) is far faster than iterrows()
    # or a separate groupby().apply() pass when there are many distinct pairs.
    for a, b, weight, has_fwd, has_bwd, label, value_sum in agg.itertuples(index=False, name=None):
        key = (a, b)
        entry = raw_edges.get(key)
        if entry is None:
            entry = {'total_weight': 0, 'has_fwd': False, 'has_bwd': False, 'type': edge_type, 'label': None, 'value_sum': 0.0}
            raw_edges[key] = entry
        entry['total_weight'] += int(weight)
        entry['has_fwd'] = entry['has_fwd'] or bool(has_fwd)
        entry['has_bwd'] = entry['has_bwd'] or bool(has_bwd)
        entry['value_sum'] += float(value_sum)
        if entry['label'] is None and pd.notna(label):
            entry['label'] = label


def _lookup_node_records(node_id, node_type, cap):
    """
    On-demand version of the old bulk node-record builder: computes the
    interaction records for exactly ONE node, only when its detail panel is
    opened. This is what keeps the main graph payload small - we no longer
    pre-build and ship logs/txns for every node whether or not it gets
    clicked.
    """
    records = []
    total = 0
    target_mode = 'phone' if node_type == 'phone' else 'bank'

    for filename in app.global_active_files:
        config = app.file_configs.get(filename)
        df = app.cleaned_dfs.get(filename)
        if df is None or not config or config.get('mode') != target_mode:
            continue

        src_col = config.get('source_col')
        tgt_col = config.get('target_col')
        if not src_col or not tgt_col or src_col not in df.columns or tgt_col not in df.columns:
            continue

        if target_mode == 'phone':
            working = _valid_phone_frame(df, src_col, tgt_col)
        else:
            working = df.dropna(subset=[src_col, tgt_col])

        mask = (working[src_col] == node_id) | (working[tgt_col] == node_id)
        matched = working[mask]
        if matched.empty:
            continue
        total += len(matched)

        if len(records) >= cap:
            continue  # still need to count `total` across remaining files

        remaining = cap - len(records)
        sub = matched.head(remaining)

        if target_mode == 'phone':
            extra_cols = {'date': config.get('dt_col'), 'time': config.get('time_col'), 'type': config.get('type_col'), 'duration': config.get('dur_col')}
        else:
            extra_cols = {'date': config.get('dt_col'), 'time': config.get('time_col'), 'amount': config.get('dur_col')}

        rec_df = pd.DataFrame({
            'from': sub[src_col].to_numpy(),
            'to': sub[tgt_col].to_numpy(),
        })
        for out_key, col in extra_cols.items():
            rec_df[out_key] = _safe_col(sub, col).to_numpy()

        records.extend(rec_df.to_dict('records'))

    return records, total


def _compute_graph_data():
    """Runs the vectorized aggregation passes over every active file and
    returns the raw graph data (node counts, edges, adjacency) with no
    presentation logic. Shared by the main graph builder and by on-demand
    endpoints (expand-member) that need the full, ungrouped picture."""
    phone_counts = defaultdict(int)
    bank_counts = defaultdict(int)
    raw_edges = {}
    phone_to_bank_map = defaultdict(set)
    cross_edges = set()
    node_files = defaultdict(set)  # node id -> set of filenames it appears in
    file_display_names = {}  # filename -> friendly label (primary/most-frequent number)

    # --- Pass 1: Financial Layer Processing ---
    for filename in app.global_active_files:
        config = app.file_configs.get(filename)
        df = app.cleaned_dfs.get(filename)

        if df is None or not config or config.get('mode') != 'bank':
            continue

        src_col = config.get('source_col')
        tgt_col = config.get('target_col')
        amt_col = config.get('dur_col')
        linked_phone = config.get('linked_phone')

        if not src_col or not tgt_col or src_col not in df.columns or tgt_col not in df.columns:
            continue

        valid_df = df.dropna(subset=[src_col, tgt_col])
        if valid_df.empty:
            continue

        primary_suspect_acc = valid_df.iloc[0][src_col]
        if linked_phone and linked_phone != "none" and pd.notna(primary_suspect_acc):
            cross_edges.add((linked_phone, primary_suspect_acc))
            phone_to_bank_map[linked_phone].add(primary_suspect_acc)

        combined_series = pd.concat([valid_df[src_col], valid_df[tgt_col]])
        for acc, count in combined_series.value_counts().items():
            bank_counts[acc] += int(count)
            node_files[acc].add(filename)

        if pd.notna(primary_suspect_acc):
            file_display_names[filename] = str(primary_suspect_acc)

        _aggregate_pairs(valid_df, src_col, tgt_col, amt_col, 'financial', raw_edges)

    # --- Pass 2: Telecom CDR Matrix Processing ---
    for filename in app.global_active_files:
        config = app.file_configs.get(filename)
        df = app.cleaned_dfs.get(filename)

        if df is None or not config or config.get('mode') != 'phone':
            continue

        src_col = config.get('source_col')
        tgt_col = config.get('target_col')
        dur_col = config.get('dur_col')

        if not src_col or not tgt_col or src_col not in df.columns or tgt_col not in df.columns:
            continue

        # Data-cleaning: normalize to clean 10-digit numbers, drop anything
        # that isn't (garbage entries, short codes, malformed OCR/export junk).
        valid_df = _valid_phone_frame(df, src_col, tgt_col)
        if valid_df.empty:
            continue

        combined_series = pd.concat([valid_df[src_col], valid_df[tgt_col]])
        value_counts = combined_series.value_counts()
        for num, count in value_counts.items():
            phone_counts[num] += int(count)
            node_files[num].add(filename)

        if not value_counts.empty:
            # The most-frequent number in a single target's CDR dump is
            # almost always the target's own number (their outgoing +
            # incoming legs together outweigh any single contact).
            file_display_names[filename] = str(value_counts.idxmax())

        _aggregate_pairs(valid_df, src_col, tgt_col, dur_col, 'telecom', raw_edges)

    neighbor_sets = defaultdict(set)
    for (a, b) in raw_edges.keys():
        neighbor_sets[a].add(b)
        neighbor_sets[b].add(a)

    return {
        'phone_counts': phone_counts, 'bank_counts': bank_counts,
        'raw_edges': raw_edges, 'neighbor_sets': neighbor_sets,
        'node_files': node_files, 'file_display_names': file_display_names,
        'phone_to_bank_map': phone_to_bank_map, 'cross_edges': cross_edges,
    }


def _compute_grouping(graph_data, grouping_config):
    """
    Decides which low-activity nodes get collapsed into a single summary
    node attached to their most significant neighbour ("hub"), so a suspect
    with hundreds of one-off contacts doesn't force the browser to render
    (and physics-layout) hundreds of individual leaf nodes.

    Rules:
      - Linked/flagged nodes (phone<->bank cross-links) are NEVER grouped -
        they're investigatively significant regardless of volume.
      - A node is only grouped if ALL of its edges fall below the threshold.
        If it has even one edge that clears the bar, it's shown individually
        with its real connections intact ("has a CDR record worth mapping").
      - Investigators can also manually pull any specific member out of a
        group via the group's detail panel (/expand-member).

    Returns (groupable_node_ids: set, hub_for_node: dict).
    """
    if not grouping_config or not grouping_config.get('enabled'):
        return set(), {}

    metric = grouping_config.get('metric', 'count')
    try:
        threshold = float(grouping_config.get('threshold') or 0)
    except (TypeError, ValueError):
        threshold = 0
    if threshold <= 0:
        return set(), {}

    raw_edges = graph_data['raw_edges']
    phone_to_bank_map = graph_data['phone_to_bank_map']
    cross_edges = graph_data['cross_edges']

    adjacency = defaultdict(list)
    for key, edata in raw_edges.items():
        a, b = key
        adjacency[a].append((b, edata))
        adjacency[b].append((a, edata))

    protected = set(phone_to_bank_map.keys())
    for members in phone_to_bank_map.values():
        protected.update(members)
    for ph, bk in cross_edges:
        protected.add(ph)
        protected.add(bk)

    def edge_value(edata):
        if metric == 'duration' and edata['type'] == 'telecom':
            return edata.get('value_sum', 0) / 60.0  # assume seconds -> minutes
        return edata['total_weight']

    groupable = set()
    hub_for_node = {}

    all_nodes = list(graph_data['phone_counts'].keys()) + list(graph_data['bank_counts'].keys())
    for node in all_nodes:
        if node in protected:
            continue
        edges = adjacency.get(node)
        if not edges:
            continue  # isolated node - nothing to attach a group edge to, leave as-is

        if all(edge_value(edata) < threshold for (_, edata) in edges):
            best_neighbor, _ = max(edges, key=lambda e: e[1]['total_weight'])
            groupable.add(node)
            hub_for_node[node] = best_neighbor

    return groupable, hub_for_node


def process_active_files_fast():
    """Builds the renderable graph: vectorized aggregation (fast even on
    large multi-file batches) + on-demand phone-number cleaning + optional
    low-activity node grouping (keeps the browser from choking on graphs
    with thousands of one-off contacts).

    NOTE: node payloads intentionally do NOT embed full interaction records
    (logs/txns) - those are fetched on demand via /node-details when a node
    is clicked, to keep this response small.

    Returns (elements, meta).
    """
    graph_data = _compute_graph_data()
    phone_counts = graph_data['phone_counts']
    bank_counts = graph_data['bank_counts']
    raw_edges = graph_data['raw_edges']
    neighbor_sets = graph_data['neighbor_sets']
    phone_to_bank_map = graph_data['phone_to_bank_map']
    cross_edges = graph_data['cross_edges']

    groupable, hub_for_node = _compute_grouping(graph_data, app.grouping_config)

    elements = []

    node_files = graph_data['node_files']

    # Format Phone Nodes (skip anything collapsed into a group) - size
    # reflects number of DISTINCT connections, not raw call volume.
    for num, total_interactions in phone_counts.items():
        if num in groupable:
            continue
        degree = len(neighbor_sets.get(num, ()))
        calculated_size = min(16 + (degree * 4), 60)
        is_linked = num in phone_to_bank_map
        elements.append({
            'data': {
                'id': num, 'phone_number': num, 'total_interactions': total_interactions,
                'connections': degree, 'size': calculated_size, 'node_type': 'phone',
                'linked_banks': list(phone_to_bank_map[num]) if is_linked else [],
                'source_files': list(node_files.get(num, [])),
                'tag': app.node_tags.get(num, '')
            },
            'classes': "phone_orange" if is_linked else "phone_standard"
        })

    # Format Bank Nodes
    for acc, total_transactions in bank_counts.items():
        if acc in groupable:
            continue
        degree = len(neighbor_sets.get(acc, ()))
        calculated_size = min(18 + (degree * 5), 65)
        elements.append({
            'data': {
                'id': acc, 'account_number': acc, 'total_transactions': total_transactions,
                'connections': degree, 'size': calculated_size, 'node_type': 'bank',
                'source_files': list(node_files.get(acc, [])),
                'tag': app.node_tags.get(acc, '')
            },
            'classes': 'bank_node'
        })

    # Build low-activity group buckets, keyed by (hub, member_type)
    group_buckets = defaultdict(lambda: {'members': [], 'total': 0})
    for node in groupable:
        hub = hub_for_node[node]
        member_type = 'phone' if node in phone_counts else 'bank'
        vol = phone_counts.get(node, 0) if member_type == 'phone' else bank_counts.get(node, 0)
        bucket = group_buckets[(hub, member_type)]
        bucket['members'].append(node)
        bucket['total'] += vol

    for (hub, member_type), bucket in group_buckets.items():
        gid = f"group_{hub}_{member_type}"
        noun = 'Contacts' if member_type == 'phone' else 'Accounts'
        elements.append({
            'data': {
                'id': gid, 'node_type': 'group', 'member_type': member_type,
                'group_count': len(bucket['members']), 'member_ids': bucket['members'],
                'total_interactions': bucket['total'], 'connections': len(bucket['members']),
                'size': min(20 + len(bucket['members']) * 2, 70),
                'group_label': f"{len(bucket['members'])} Low-Activity {noun}"
            },
            'classes': 'group_node'
        })
        elements.append({
            'data': {
                'id': f"{gid}_edge", 'source': hub, 'target': gid,
                'weight': f"{len(bucket['members'])} grouped", 'numeric_weight': bucket['total'],
                'arrow_mode': 'none', 'edge_type': 'group_link', 'is_top_3': False
            },
            'classes': 'group_link_edge'
        })

    # Edges between two normally-rendered nodes only (edges touching a
    # grouped node are represented by the aggregate hub->group edge above).
    displayable_edges = {
        key: edata for key, edata in raw_edges.items()
        if key[0] not in groupable and key[1] not in groupable
    }

    top_3_edge_keys = set(sorted(displayable_edges.keys(), key=lambda k: displayable_edges[k]['total_weight'], reverse=True)[:3])

    for i, (bi_key, edge_data) in enumerate(displayable_edges.items()):
        node_a, node_b = bi_key
        weight = edge_data['total_weight']
        edge_type = edge_data['type']

        has_a_to_b = edge_data['has_fwd']
        has_b_to_a = edge_data['has_bwd']
        arrow_mode = "both" if (has_a_to_b and has_b_to_a) else ("forward" if has_a_to_b else "backward")

        display_label = f"₹{edge_data['label']} ({weight}tx)" if (edge_type == 'financial' and edge_data['label']) else f"{weight} calls"

        is_top_3 = bi_key in top_3_edge_keys
        edge_class = 'financial_edge' if edge_type == 'financial' else 'telecom_edge'
        if is_top_3:
            edge_class += ' top_edge_highlight'

        elements.append({
            'data': {
                'id': f"edge_{i}", 'source': node_a, 'target': node_b,
                'weight': display_label, 'numeric_weight': weight,
                'arrow_mode': arrow_mode, 'edge_type': edge_type,
                'is_top_3': is_top_3
            },
            'classes': edge_class
        })

    # Format Cross Links (phone <-> linked bank account) - never grouped
    for idx, (ph_node, bk_node) in enumerate(cross_edges):
        elements.append({
            'data': {
                'id': f"cross_tether_{idx}", 'source': ph_node, 'target': bk_node,
                'weight': 'LINKED ASSET', 'arrow_mode': 'none', 'edge_type': 'cross_link'
            },
            'classes': 'cross_link_edge'
        })

    app.file_display_names = graph_data['file_display_names']

    meta = {
        'rendered_elements': len(elements),
        'grouped_count': len(groupable),
        'file_labels': app.file_display_names,
        'warning': (
            f"Graph has {len(elements)} elements - this may still be slow to render. "
            f"Consider enabling/raising the low-activity grouping threshold."
            if len(elements) > RENDER_WARNING_THRESHOLD else None
        )
    }

    return elements, meta


def get_existing_phone_numbers():
    """Distinct (cleaned) phone numbers already staged, used to populate the link-to-suspect dropdown."""
    phones = set()
    for filename in app.global_active_files:
        config = app.file_configs.get(filename)
        df = app.cleaned_dfs.get(filename)
        if df is None or not config or config.get('mode') != 'phone':
            continue
        src_col, tgt_col = config.get('source_col'), config.get('target_col')
        if not src_col or not tgt_col or src_col not in df.columns or tgt_col not in df.columns:
            continue
        valid = _valid_phone_frame(df, src_col, tgt_col)
        phones.update(valid[src_col].unique().tolist())
        phones.update(valid[tgt_col].unique().tolist())
    return sorted(phones)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files' not in request.files:
        return jsonify({'error': 'No file element detected.'}), 400

    uploaded_files = request.files.getlist('files')
    if not uploaded_files or uploaded_files[0].filename == '':
        return jsonify({'error': 'No document selected for staging.'}), 400

    filenames = []
    auto_mappings = {}
    primary_headers = []

    try:
        for idx, file in enumerate(uploaded_files):
            if file and file.filename.endswith('.csv'):
                stream = file.read().decode("utf-8", errors="ignore")
                filename = file.filename

                df = pd.read_csv(io.StringIO(stream), index_col=False, dtype=str)
                df = clean_dataframe(df)

                app.cleaned_dfs[filename] = df
                filenames.append(filename)

                if idx == 0:
                    auto_mappings = auto_detect_columns(df.columns.tolist())
                    primary_headers = df.columns.tolist()

        return jsonify({
            'filenames': filenames,
            'headers': primary_headers,
            'auto_mapping': auto_mappings,
            'existing_phones': get_existing_phone_numbers(),
            'grouping_config': app.grouping_config
        })
    except Exception as e:
        return jsonify({'error': f'Failed reading and cleaning CSV stream: {str(e)}'}), 500


def _graph_response():
    """Common envelope builder: {'files': [...], 'graph': [...], 'graph_meta': {...}}"""
    elements, meta = process_active_files_fast()
    return jsonify({'files': app.global_active_files, 'graph': elements, 'graph_meta': meta})


@app.route('/process-batch-mapping', methods=['POST'])
def process_batch_mapping():
    data = request.json
    filenames = data.get('filenames', [])

    for filename in filenames:
        app.file_configs[filename] = {
            'mode': data.get('mode', 'phone'),
            'source_col': data.get('source_col'),
            'target_col': data.get('target_col'),
            'dur_col': data.get('dur_col'),
            'type_col': data.get('type_col'),
            'dt_col': data.get('dt_col'),
            'time_col': data.get('time_col'),
            'linked_phone': data.get('linked_phone')
        }
        if filename not in app.global_active_files:
            app.global_active_files.append(filename)

    if 'group_enabled' in data:
        app.grouping_config = {
            'enabled': bool(data.get('group_enabled')),
            'metric': data.get('group_metric', 'count'),
            'threshold': data.get('group_threshold', 0),
        }

    return _graph_response()


@app.route('/node-details/<path:node_id>', methods=['GET'])
def node_details(node_id):
    node_type = request.args.get('type', 'phone')
    if node_type not in ('phone', 'bank'):
        return jsonify({'error': 'Invalid node type.'}), 400

    records, total = _lookup_node_records(node_id, node_type, MAX_LOGS_PER_NODE)
    return jsonify({'records': records, 'total': total})


@app.route('/expand-member/<path:node_id>', methods=['GET'])
def expand_member(node_id):
    """
    Pulls one specific node out of its low-activity group and returns its
    real node + edge data (uncapped by the grouping decision), so an
    investigator who spots something worth mapping in a grouped member's
    records can bring it into the canvas individually.
    """
    node_type = request.args.get('type', 'phone')
    if node_type not in ('phone', 'bank'):
        return jsonify({'error': 'Invalid node type.'}), 400

    graph_data = _compute_graph_data()
    counts = graph_data['phone_counts'] if node_type == 'phone' else graph_data['bank_counts']
    if node_id not in counts:
        return jsonify({'error': 'Node not found.'}), 404

    neighbor_sets = graph_data['neighbor_sets']
    raw_edges = graph_data['raw_edges']
    phone_to_bank_map = graph_data['phone_to_bank_map']

    degree = len(neighbor_sets.get(node_id, ()))
    node_files = graph_data['node_files']
    if node_type == 'phone':
        is_linked = node_id in phone_to_bank_map
        node_elem = {
            'data': {
                'id': node_id, 'phone_number': node_id, 'total_interactions': counts[node_id],
                'connections': degree, 'size': min(16 + (degree * 4), 60), 'node_type': 'phone',
                'linked_banks': list(phone_to_bank_map[node_id]) if is_linked else [],
                'source_files': list(node_files.get(node_id, [])),
                'tag': app.node_tags.get(node_id, '')
            },
            'classes': "phone_orange" if is_linked else "phone_standard"
        }
    else:
        node_elem = {
            'data': {
                'id': node_id, 'account_number': node_id, 'total_transactions': counts[node_id],
                'connections': degree, 'size': min(18 + (degree * 5), 65), 'node_type': 'bank',
                'source_files': list(node_files.get(node_id, [])),
                'tag': app.node_tags.get(node_id, '')
            },
            'classes': 'bank_node'
        }

    edge_elems = []
    for i, (bi_key, edata) in enumerate(raw_edges.items()):
        if node_id not in bi_key:
            continue
        node_a, node_b = bi_key
        weight = edata['total_weight']
        arrow_mode = "both" if (edata['has_fwd'] and edata['has_bwd']) else ("forward" if edata['has_fwd'] else "backward")
        display_label = f"₹{edata['label']} ({weight}tx)" if (edata['type'] == 'financial' and edata['label']) else f"{weight} calls"
        edge_class = 'financial_edge' if edata['type'] == 'financial' else 'telecom_edge'
        edge_elems.append({
            'data': {
                'id': f"expand_{node_id}_edge_{i}", 'source': node_a, 'target': node_b,
                'weight': display_label, 'numeric_weight': weight,
                'arrow_mode': arrow_mode, 'edge_type': edata['type'], 'is_top_3': False
            },
            'classes': edge_class
        })

    return jsonify({'node': node_elem, 'edges': edge_elems})


@app.route('/tag/<path:node_id>', methods=['POST'])
def set_tag(node_id):
    """Assigns an investigator-chosen name/tag to a number or account (e.g.
    'Suspect A', 'Mule Account 3'). Global across the case, like grouping."""
    data = request.json or {}
    tag = (data.get('tag') or '').strip()
    if tag:
        app.node_tags[node_id] = tag
    else:
        app.node_tags.pop(node_id, None)
    return jsonify({'node_id': node_id, 'tag': app.node_tags.get(node_id, '')})


@app.route('/get-current-state', methods=['GET'])
def get_current_state():
    return _graph_response()


@app.route('/delete/<path:filename>', methods=['DELETE'])
def delete_file(filename):
    app.cleaned_dfs.pop(filename, None)
    app.file_configs.pop(filename, None)
    if filename in app.global_active_files:
        app.global_active_files.remove(filename)

    return _graph_response()


@app.route('/export-workspace', methods=['GET'])
def export_workspace():
    bundle = {
        'version': 1,
        'active_files': app.global_active_files,
        'file_configs': app.file_configs,
        'grouping_config': app.grouping_config,
        'node_tags': app.node_tags,
        'datasets': {
            filename: df.to_csv(index=False)
            for filename, df in app.cleaned_dfs.items()
            if filename in app.global_active_files
        }
    }
    return jsonify(bundle)


@app.route('/import-workspace', methods=['POST'])
def import_workspace():
    if 'workspace_file' not in request.files:
        return jsonify({'error': 'No workspace file provided.'}), 400

    try:
        raw = request.files['workspace_file'].read().decode('utf-8', errors='ignore')
        bundle = json.loads(raw)

        for filename, csv_text in bundle.get('datasets', {}).items():
            df = pd.read_csv(io.StringIO(csv_text), index_col=False, dtype=str)
            df = clean_dataframe(df)
            app.cleaned_dfs[filename] = df

        app.file_configs.update(bundle.get('file_configs', {}))
        if 'grouping_config' in bundle:
            app.grouping_config = bundle['grouping_config']
        if 'node_tags' in bundle:
            app.node_tags.update(bundle['node_tags'])

        for filename in bundle.get('active_files', []):
            if filename not in app.global_active_files:
                app.global_active_files.append(filename)

        return _graph_response()
    except Exception as e:
        return jsonify({'error': f'Failed to restore workspace: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(debug=True, port=80)