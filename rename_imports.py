import os
import re

files_to_update = [
    'tests/test_alerts_pipeline.py',
    'tests/test_backpressure.py',
    'tests/test_bug_fixes.py',
    'tests/test_checkpoint.py',
    'tests/test_connectors.py',
    'tests/test_duckdb_sql.py',
    'tests/test_engine.py',
    'tests/test_memory_budget.py',
    'tests/test_metrics.py',
    'tests/test_optimizations.py',
    'tests/test_quack_integration.py',
    'tests/test_retention.py',
    'tests/test_schema.py',
    'tests/test_stream_joins.py',
    'tests/test_table_modes.py',
    'tests/test_upsert.py',
    'examples/benchmark_streams.py',
    'examples/live_streaming_demo.py',
    'examples/multi_stream_join_demo.py',
    'examples/quack_dashboard_demo.py',
    'dashboard/quack_worker.py',
    'README.md',
    'CONTRIBUTING.md',
    'rills/table.py',
]

def replace_rill(text):
    # import rill -> import rills
    text = re.sub(r'\bimport rill\b', 'import rills', text)
    # from rill import -> from rills import
    text = re.sub(r'\bfrom rill\b', 'from rills', text)
    # from rill.something -> from rills.something
    text = re.sub(r'\bfrom rill\.', 'from rills.', text)
    # pip install rill -> pip install rills
    text = re.sub(r'\bpip install rill\b', 'pip install rills', text)
    # The Rill Streaming Engine (rill) -> (rills)
    text = re.sub(r'Engine \(rill\)', 'Engine (rills)', text)
    # github.com/lijose/rill is kept as-is, wait, no, the prompt says:
    # "Decide on GitHub repo name: keep github.com/lijose/rill as-is ... or rename. If renaming, update pyproject.toml"
    # I'll keep the repo as-is, so URLs with `rill` are not changed.
    return text

for file_path in files_to_update:
    full_path = os.path.join('/home/lijo/Documents/codes/rill', file_path)
    if os.path.exists(full_path):
        with open(full_path, 'r') as f:
            content = f.read()
        
        new_content = replace_rill(content)
        
        if new_content != content:
            with open(full_path, 'w') as f:
                f.write(new_content)
            print(f'Updated {file_path}')
