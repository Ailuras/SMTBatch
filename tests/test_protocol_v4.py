import json
from smtbatch import reduce, predicate
from smtbatch.oracle_protocol import decode, preserving


def envelope(outcome='interesting',signature='crash'):
    return dict(schema='d3smt-oracle-v3',outcome=outcome,reason='test',signature=signature)


def test_protocol_is_fail_closed():
    assert decode(json.dumps(envelope()),0)
    assert decode(json.dumps(envelope()),1) is None
    assert decode('matched',0) is None
    assert not preserving(envelope('incomplete'),envelope('incomplete'))
    assert not preserving(envelope(),envelope(signature='other'))
    assert not reduce._matches_baseline(dict(oracle=envelope('not_interesting')),dict(oracle=envelope('not_interesting')), {})
    assert not reduce._matches_baseline(dict(timed_out=True),dict(timed_out=True),{})


def test_ast_then_normalized_bytes(tmp_path):
    a=tmp_path/'a.smt2'; a.write_text('(assert (and true true true true))')
    b=tmp_path/'b.smt2'; b.write_text('(assert true)\n(check-sat)')
    qa=predicate._candidate_record(a)['quality']; qb=predicate._candidate_record(b)['quality']
    assert qa['expression_count']<qb['expression_count']
    assert reduce._rank_quality(reduce._quality(qa))>reduce._rank_quality(reduce._quality(qb))
    a.write_text('(assert true)'); qa=predicate._candidate_record(a)['quality']
    a.write_text('  ( assert   true )  \n'); qb=predicate._candidate_record(a)['quality']
    assert qa['byte_count']!=qb['byte_count']
    assert reduce._rank_quality(reduce._quality(qa))==reduce._rank_quality(reduce._quality(qb))


def test_parse_failure_retains_raw_identity(tmp_path):
    p=tmp_path/'broken.smt2'; p.write_text('(')
    r=predicate._candidate_record(p)
    assert r['quality'] is None and r['bytes']==1 and r['sha256']


def test_duplicate_repeat_cannot_replace_missing_pair():
    row=dict(benchmark_id='a',reducer_id='r',repeat=1,verified=True,evidence_ok=True,
             output_quality=dict(node_count=8,expression_count=2,byte_count=20))
    assert reduce._case_reducer_quality([row,row],'a','r',2) is None


def test_missing_trajectory_quality_is_not_zero(tmp_path):
    r=reduce.trajectory_for_attempt(tmp_path,dict(input_bytes=12,predicate=dict(match={})),{},allow_partial=True)
    assert r['initial'] is None and r['accepted_best'] is None
    assert r['points'][0]['node_count'] is None


def test_repeat_pairing_precedes_case_aggregation():
    rows=[]
    for arm,values in [('left',[1,99,100]),('right',[2,98,101])]:
        for repeat,size in enumerate(values,1):
            rows.append(dict(benchmark_id='a',reducer_id=arm,repeat=repeat,verified=True,evidence_ok=True,
                             output_quality=dict(expression_count=1,node_count=size,byte_count=size,normalized_byte_count=size)))
    plan=dict(comparisons=[['left','right']],benchmarks=[dict(id='a')],repeats=3)
    result=reduce.comparison_rows(plan,rows)[0]
    assert result['wins']==1 and result['paired_trials']==3


def test_incomplete_evidence_is_not_automatically_timeout():
    record=envelope('incomplete');record.update(target=None,reference=None)
    run=dict(stdout=json.dumps(record).encode(),stderr=b'',returncode=3,timed_out=False,wall_sec=0,error=None)
    assert reduce._observation(run)['timed_out'] is False
    record['target']={'status':'timeout'};run['stdout']=json.dumps(record).encode()
    assert reduce._observation(run)['timed_out'] is True
