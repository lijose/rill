"""Regression coverage for the dependency and correctness review."""
import datetime
import threading
import time
import importlib.util
from pathlib import Path
import duckdb
import pyarrow as pa
import pytest
from rills import RillEngine, RillTable, RetentionPolicy, MemoryConnector, JSONStreamConnector
from rills.compute.sql import ScheduledSQLTask
from rills.compute.upsert import upsert_table
from rills.sql_validation import validate_read_only_sql


@pytest.fixture
def engine():
    with RillEngine(metrics_db_path=':memory:') as engine:
        yield engine


def test_query_metrics_arrow_and_read_only(engine):
    assert engine.query_metrics('SELECT 42 AS answer').to_pylist() == [{'answer':42}]
    with pytest.raises(ValueError):
        engine.query_metrics('SELECT 1; DELETE FROM rill_metrics')
    with pytest.raises(ValueError):
        engine.query_metrics('DELETE FROM rill_metrics')


@pytest.mark.parametrize('keys', ['id', ['id', 'group']])
@pytest.mark.parametrize('order', [[3,2,1], [2,3,1], [1,2,3]])
def test_upsert_timestamps_stay_with_keys(keys, order):
    old = pa.table({'id':[1,2], 'group':['a','a'], 'value':[10,20],
                    'z_insert_ts':[10.,20.], 'z_update_ts':[10.,20.]})
    incoming = pa.table({'id':order,'group':['a']*3,'value':[i*100 for i in order]})
    result = upsert_table(old, incoming, keys, current_time=30.)
    assert result['id'].to_pylist() == order
    assert {r['id']:r['z_insert_ts'] for r in result.to_pylist()} == {1:10.,2:20.,3:30.}
    assert all(r['value'] == r['id'] * 100 and r['z_update_ts'] == 30. for r in result.to_pylist())
    retained = RetentionPolicy(max_age_seconds=15).apply(result, current_time=30.)
    assert set(retained['id'].to_pylist()) == {2,3}


def test_dedup_retains_last_value_and_its_input_position():
    result = upsert_table(None, pa.table({'id':[1,2,1,3], 'v':[10,20,30,40]}), 'id')
    assert result.select(['id','v']).to_pylist() == [{'id':2,'v':20},{'id':1,'v':30},{'id':3,'v':40}]


def test_chained_tasks_current_tick_and_catalog(engine):
    raw=engine.register_table('raw',primary_key='id')
    raw.upsert(pa.table({'id':[1],'v':[10]}))
    engine.add_sql_task(ScheduledSQLTask('a','SELECT id,v+1 AS v FROM raw','stage1',0))
    engine.add_sql_task(ScheduledSQLTask('b','SELECT id,v+1 AS v FROM stage1','stage2',0))
    engine.step()
    assert engine._tables['stage2'].to_arrow()['v'].to_pylist() == [12]
    raw.upsert(pa.table({'id':[1],'v':[100]}));engine.step()
    assert engine._tables['stage2'].to_arrow()['v'].to_pylist() == [102]
    names={r[0] for r in engine.metrics_con.execute('SELECT table_name FROM rill_tables').fetchall()}
    assert {'stage1','stage2'} <= names
    engine.metrics_con.execute("UPDATE stage1 SET insert_ts='2000-01-01T00:00:00'")
    engine.remove_sql_task(engine.sql_tasks[0]);engine.step()
    assert engine.metrics_con.execute('SELECT count(*) FROM stage1').fetchone()[0] == 0


@pytest.mark.parametrize('sql', ['SELECT 1; DELETE FROM audit', 'WITH x AS (SELECT 1) DELETE FROM audit',
    'EXPLAIN ANALYZE DELETE FROM audit', 'PRAGMA enable_profiling', '/*x*/ SELECT 1; DROP TABLE audit'])
def test_read_only_rejects_mutation(sql):
    with duckdb.connect() as con:
        con.execute('CREATE TABLE audit(id INT); INSERT INTO audit VALUES (1)')
        with pytest.raises(ValueError): validate_read_only_sql(con,sql)
        assert con.execute('SELECT * FROM audit').fetchall() == [(1,)]


@pytest.mark.parametrize('sql', ['SELECT \'; DROP TABLE x;\' AS value', 'WITH x AS (SELECT 1) SELECT * FROM x',
    'SHOW TABLES','PRAGMA show_tables','DESCRIBE SELECT 1','EXPLAIN ANALYZE SELECT 1'])
def test_read_only_accepts_reads(sql):
    with duckdb.connect() as con:validate_read_only_sql(con,sql)


def test_worker_does_not_patch_json_and_rejects_multi_statement():
    import json
    original=json.dumps
    spec=importlib.util.spec_from_file_location('worker',Path(__file__).parents[1]/'dashboard/quack_worker.py')
    worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(worker)
    assert json.dumps is original
    with duckdb.connect() as con:
        with pytest.raises(ValueError):worker.execute_query(con,'SELECT 1; CREATE TABLE x(id INT)')


def test_alert_write_failure_preserves_batch_and_recovers(engine):
    class FailingConnection:
        def __getattr__(self,name):return getattr(engine.metrics_con,name)
        def executemany(self,sql,values):
            if 'INSERT INTO rill_alerts' in sql:
                engine.alert('arrived during flush')
                raise RuntimeError('temporary failure')
            return engine.metrics_con.executemany(sql,values)
    engine.alert('pending')
    engine.metrics.write_to_duckdb(FailingConnection(),engine)
    assert len(engine.metrics._alert_buffer)==2
    engine.step()
    assert engine.metrics_con.execute('SELECT message FROM rill_alerts ORDER BY ts').fetchall()==[('pending',),('arrived during flush',)]
    assert not engine.metrics._alert_buffer


def test_alert_retention_runs_without_new_alerts(engine):
    now=datetime.datetime.now()
    for minutes in [30,120]:
        engine.metrics_con.execute('INSERT INTO rill_alerts VALUES (?,?,?,?,?)',[(now-datetime.timedelta(minutes=minutes)).isoformat(),'info','test',str(minutes),''])
    engine.step()
    assert engine.metrics_con.execute('SELECT message FROM rill_alerts').fetchall()==[('30',)]


def test_memory_connector_missing_keys_and_rejected_push():
    con=MemoryConnector('t',max_buffer_records=2,on_overflow='error')
    con.push([{'id':1},{'id':2,'name':'B'}])
    with pytest.raises(BufferError):con.push({'id':[3]})
    assert con.read_batch().to_pylist()==[{'id':1,'name':None},{'id':2,'name':'B'}]


@pytest.mark.parametrize('policy,expected',[('drop_oldest',[2,3]),('drop_newest',[1,2])])
def test_json_limits_records_and_bytes(policy,expected):
    con=JSONStreamConnector('t',max_buffer_records=2,max_buffer_bytes=18,on_overflow=policy)
    con.push_bytes('{"id":1}\n{"id":2}\n{"id":3}\n')
    assert con._total_bytes<=18
    assert con.read_batch()['id'].to_pylist()==expected
    con.push_bytes('{"large":"'+'x'*100+'"}')
    assert con.read_batch() is None


def test_json_error_is_atomic_including_delimiter():
    con=JSONStreamConnector('t',max_buffer_bytes=18,on_overflow='error')
    con.push_bytes('{"id":1}')
    with pytest.raises(BufferError):con.push_bytes('{"id":2}\n{"id":3}')
    assert con.read_batch()['id'].to_pylist()==[1]


@pytest.mark.parametrize('kwargs',[{'max_rows':0},{'max_rows':-1},{'max_rows':1.5},{'max_age_seconds':0},{'max_age_seconds':float('nan')}])
def test_invalid_retention_rejected(kwargs):
    with pytest.raises(ValueError):RetentionPolicy(**kwargs)


def test_invalid_retention_column_does_not_disable_ttl():
    policy=RetentionPolicy(max_age_seconds=10,time_column='ts')
    with pytest.raises(ValueError):policy.apply(pa.table({'id':[1]}))
    with pytest.raises(ValueError):policy.apply(pa.table({'ts':['invalid']}))


def test_synchronous_stop_closes_owned_connections():
    engine=RillEngine(metrics_db_path=':memory:');engine.step();engine.stop();engine.stop()
    with pytest.raises(duckdb.Error):engine.metrics_con.execute('SELECT 1')
    with pytest.raises(duckdb.Error):engine.duckdb.con.execute('SELECT 1')
    with pytest.raises(RuntimeError):engine.start()


def test_supplied_connection_remains_open():
    with duckdb.connect() as con:
        engine=RillEngine(duckdb_connection=con,metrics_db_path=':memory:');engine.stop()
        assert con.execute('SELECT 1').fetchone()==(1,)


def test_concurrent_metrics_reads_and_steps(engine):
    failures=[]
    def reads():
        try:
            for _ in range(15):assert engine.query_metrics('SELECT count(*) FROM rill_metrics').num_rows==1
        except Exception as exc:failures.append(exc)
    thread=threading.Thread(target=reads);thread.start()
    for _ in range(15):engine.step()
    thread.join(5)
    assert not thread.is_alive() and not failures


def test_loop_failure_sets_health(engine,monkeypatch):
    def fail():raise RuntimeError('controlled failure')
    monkeypatch.setattr(engine,'step',fail)
    engine.start();engine._thread.join(2)
    assert not engine._running
    assert str(engine.last_error)=='controlled failure'


def test_memory_measurement_is_numeric(engine):
    engine.step()
    values=engine.metrics_con.execute("SELECT value FROM rill_metrics WHERE metric_name IN ('duckdb_live_bytes','duckdb_metrics_bytes')").fetchall()
    assert len(values)==2 and all(value is not None and value>=0 for value, in values)
