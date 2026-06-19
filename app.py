import os
import io
import json
import pandas as pd
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)
app.secret_key = "cyber_cell_secret_key_secure"

# Volatile Data-Cache structures safely isolated per process-runtime lifecycle
if not hasattr(app, 'file_configs'):
    app.file_configs = {}
if not hasattr(app, 'global_active_files'):
    app.global_active_files = []
if not hasattr(app, 'phone_to_bank_map'):
    app.phone_to_bank_map = {}
# In-memory virtual layer dictionary to bypass structural disk storage constraints
if not hasattr(app, 'virtual_file_storage'):
    app.virtual_file_storage = {}

def process_active_files(active_files):
    nodes_dict = {}     
    bank_nodes = {}     
    raw_edges = {}      
    
    app.phone_to_bank_map = {}
    cross_edges = set() 

    # Pass 1: Financial Layer Processing
    for filename in active_files:
        config = app.file_configs.get(filename)
        csv_content = app.virtual_file_storage.get(filename)
        if not csv_content or not config or config.get('mode') != 'bank':
            continue
            
        try:
            df = pd.read_csv(io.StringIO(csv_content))
            df.columns = df.columns.str.strip()
            
            src_acc_col = config['source_col']
            tgt_acc_col = config['target_col']
            amt_col = config.get('dur_col')    
            mth_col = config.get('type_col')   
            dt_col = config.get('dt_col')      
            linked_phone = config.get('linked_phone')
            
            if linked_phone and linked_phone != "none" and not df.empty:
                primary_suspect_acc = str(df.iloc[0][src_acc_col]).strip()
                if primary_suspect_acc and primary_suspect_acc != 'nan':
                    cross_edges.add((linked_phone, primary_suspect_acc))
            
            for _, row in df.iterrows():
                src_acc = str(row[src_acc_col]).strip()
                tgt_acc = str(row[tgt_acc_col]).strip()
                
                if not src_acc or not tgt_acc or src_acc == 'nan' or tgt_acc == 'nan':
                    continue
                
                for acc in [src_acc, tgt_acc]:
                    if acc not in bank_nodes:
                        bank_nodes[acc] = {'count': 0, 'txns': []}
                    bank_nodes[acc]['count'] += 1
                
                txn_detail = {
                    'date': str(row[dt_col]).strip() if dt_col and dt_col in df.columns else "N/A",
                    'method': str(row[mth_col]).strip() if mth_col and mth_col in df.columns else "N/A",
                    'amount': str(row[amt_col]).strip() if amt_col and amt_col in df.columns else "Unknown",
                    'from': src_acc, 'to': tgt_acc
                }
                bank_nodes[src_acc]['txns'].append(txn_detail)
                bank_nodes[tgt_acc]['txns'].append(txn_detail)
                
                if linked_phone and linked_phone != "none" and (src_acc == primary_suspect_acc or tgt_acc == primary_suspect_acc):
                    if linked_phone not in app.phone_to_bank_map:
                        app.phone_to_bank_map[linked_phone] = set()
                    app.phone_to_bank_map[linked_phone].add(src_acc)
                
                bi_key = tuple(sorted([src_acc, tgt_acc]))
                directed_key = (src_acc, tgt_acc)
                
                if bi_key not in raw_edges:
                    raw_edges[bi_key] = {'total_weight': 0, 'directions': set(), 'type': 'financial', 'labels': []}
                raw_edges[bi_key]['total_weight'] += 1
                raw_edges[bi_key]['directions'].add(directed_key)
                if amt_col and amt_col in df.columns:
                    raw_edges[bi_key]['labels'].append(str(row[amt_col]).strip())
        except Exception as e:
            print(f"Error parsing financial processing layer: {e}")

    # Pass 2: Telecom CDR Matrix Processing
    for filename in active_files:
        config = app.file_configs.get(filename)
        csv_content = app.virtual_file_storage.get(filename)
        if not csv_content or not config or config.get('mode') != 'phone':
            continue
            
        try:
            df = pd.read_csv(io.StringIO(csv_content))
            df.columns = df.columns.str.strip()
            
            src_col = config['source_col']
            tgt_col = config['target_col']
            dur_col = config.get('dur_col')
            type_col = config.get('type_col')
            dt_col = config.get('dt_col')
            
            for _, row in df.iterrows():
                src = str(row[src_col]).strip()
                tgt = str(row[tgt_col]).strip()
                
                if not src or not tgt or src == 'nan' or tgt == 'nan':
                    continue
                
                for num in [src, tgt]:
                    if num not in nodes_dict:
                        nodes_dict[num] = {'count': 0, 'logs': []}
                    nodes_dict[num]['count'] += 1
                
                log_entry = {
                    'timestamp': str(row[dt_col]).strip() if dt_col and dt_col in df.columns else "N/A",
                    'type': str(row[type_col]).strip() if type_col and type_col in df.columns else "N/A",
                    'duration': str(row[dur_col]).strip() if dur_col and dur_col in df.columns else "0",
                    'from': src, 'to': tgt
                }
                nodes_dict[src]['logs'].append(log_entry)
                nodes_dict[tgt]['logs'].append(log_entry)
                
                bi_key = tuple(sorted([src, tgt]))
                directed_key = (src, tgt)
                if bi_key not in raw_edges:
                    raw_edges[bi_key] = {'total_weight': 0, 'directions': set(), 'type': 'telecom'}
                raw_edges[bi_key]['total_weight'] += 1
                raw_edges[bi_key]['directions'].add(directed_key)
        except Exception as e:
            print(f"Error parsing telecom metadata records: {e}")

    elements = []
    for num, tracking in nodes_dict.items():
        base_size = 15
        calculated_size = min(base_size + (tracking['count'] * 0.8), 50)
        is_linked = num in app.phone_to_bank_map
        elements.append({
            'data': {
                'id': num, 'phone_number': num, 'total_interactions': tracking['count'],
                'logs': tracking['logs'], 'size': calculated_size, 'node_type': 'phone',
                'linked_banks': list(app.phone_to_bank_map[num]) if is_linked else []
            },
            'classes': "phone_orange" if is_linked else "phone_standard"
        })
        
    for acc, tracking in bank_nodes.items():
        base_size = 20
        calculated_size = min(base_size + (tracking['count'] * 1.2), 55)
        elements.append({
            'data': {
                'id': acc, 'account_number': acc, 'total_transactions': tracking['count'],
                'txns': tracking['txns'], 'size': calculated_size, 'node_type': 'bank'
            },
            'classes': 'bank_node'
        })
        
    for i, (bi_key, edge_data) in enumerate(raw_edges.items()):
        node_a, node_b = bi_key
        directions = edge_data['directions']
        weight = edge_data['total_weight']
        edge_type = edge_data['type']
        
        has_a_to_b = (node_a, node_b) in directions
        has_b_to_a = (node_b, node_a) in directions
        arrow_mode = "both" if (has_a_to_b and has_b_to_a) else ("forward" if has_a_to_b else "backward")

        display_label = f"₹{edge_data['labels'][0]} ({weight}tx)" if (edge_type == 'financial' and edge_data['labels']) else str(weight)

        elements.append({
            'data': {
                'id': f"edge_{i}", 'source': node_a, 'target': node_b,
                'weight': display_label, 'arrow_mode': arrow_mode, 'edge_type': edge_type
            },
            'classes': 'financial_edge' if edge_type == 'financial' else ''
        })
        
    for idx, (ph_node, bk_node) in enumerate(cross_edges):
        elements.append({
            'data': {
                'id': f"cross_tether_{idx}", 'source': ph_node, 'target': bk_node,
                'weight': 'LINKED ASSET', 'arrow_mode': 'none', 'edge_type': 'cross_link'
            },
            'classes': 'cross_link_edge'
        })
        
    return elements


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get-current-state', methods=['GET'])
def get_current_state():
    graph_data = process_active_files(app.global_active_files)
    return jsonify({
        'files': app.global_active_files,
        'graph': graph_data
    })

@app.route('/upload', methods=['POST'])
def upload_files():
    if 'files' not in request.files: 
        return jsonify({'error': 'No file element detected.'}), 400
    
    uploaded_files = request.files.getlist('files')
    if not uploaded_files or uploaded_files[0].filename == '': 
        return jsonify({'error': 'No document selected for staging.'}), 400
        
    filenames = []
    primary_headers = []
    
    try:
        for idx, file in enumerate(uploaded_files):
            if file and file.filename.endswith('.csv'):
                # Read structural content into memory dataframes cleanly
                stream = file.read().decode("utf-8")
                filename = file.filename
                
                app.virtual_file_storage[filename] = stream
                filenames.append(filename)
                
                if idx == 0:
                    df = pd.read_csv(io.StringIO(stream), nrows=2)
                    primary_headers = [str(col).strip() for col in df.columns]
        
        active_elements = process_active_files(app.global_active_files)
        phone_numbers = [e['data']['phone_number'] for e in active_elements if e['data'].get('node_type') == 'phone']
        
        return jsonify({
            'filenames': filenames, 
            'headers': primary_headers,
            'existing_phones': sorted(list(set(phone_numbers)))
        })
    except Exception as e:
        return jsonify({'error': f'Failed reading memory streams structural vectors: {str(e)}'}), 500

@app.route('/process-batch-mapping', methods=['POST'])
def process_batch_mapping():
    data = request.json
    filenames = data.get('filenames', [])
    
    # Store uniform configuration template mapping parameters matching the whole upload array execution step
    for filename in filenames:
        app.file_configs[filename] = {
            'mode': data.get('mode', 'phone'),
            'source_col': data.get('source_col'),
            'target_col': data.get('target_col'),
            'dur_col': data.get('dur_col'),
            'type_col': data.get('type_col'),
            'dt_col': data.get('dt_col'),
            'linked_phone': data.get('linked_phone')
        }
        if filename not in app.global_active_files:
            app.global_active_files.append(filename)
            
    return jsonify({'files': app.global_active_files, 'graph': process_active_files(app.global_active_files)})

@app.route('/delete/<filename>', methods=['DELETE'])
def delete_file(filename):
    if filename in app.global_active_files:
        app.global_active_files.remove(filename)
    if filename in app.file_configs:
        del app.file_configs[filename]
    if filename in app.virtual_file_storage:
        del app.virtual_file_storage[filename]
            
    return jsonify({'files': app.global_active_files, 'graph': process_active_files(app.global_active_files)})

@app.route('/import-workspace', methods=['POST'])
def import_workspace():
    try:
        file = request.files['workspace_file']
        workspace_data = json.loads(file.read().decode('utf-8'))
        
        app.file_configs = workspace_data.get('file_configs', {})
        app.global_active_files = workspace_data.get('global_active_files', [])
        app.virtual_file_storage = workspace_data.get('virtual_file_storage', {})
        
        return jsonify({'files': app.global_active_files, 'graph': process_active_files(app.global_active_files)})
    except Exception as e:
        return jsonify({'error': f'Invalid case workspace import parameters: {str(e)}'}), 400

@app.route('/export-workspace', methods=['GET'])
def export_workspace():
    workspace_bundle = {
        'file_configs': app.file_configs,
        'global_active_files': app.global_active_files,
        'virtual_file_storage': app.virtual_file_storage
    }
    return jsonify(workspace_bundle)

if __name__ == '__main__':
    app.run(debug=True, port=80)