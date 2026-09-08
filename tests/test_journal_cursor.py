import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from smtbatch import predicate


def append(path, candidate, i):
    return predicate._append_start(path, call_id=str(i), phase='reducer', candidate=candidate)


def test_cursor_recovers_missing_stale_and_corrupt_index(tmp_path):
    path, candidate = tmp_path/'predicate.jsonl', tmp_path/'input.smt2'
    candidate.write_text('(assert |中文|)\n')
    a = append(path, candidate, 1)
    saved = path.with_name(path.name+'.cursor').read_bytes()
    b = append(path, candidate, 2)
    path.with_name(path.name+'.cursor').write_bytes(saved)
    c = append(path, candidate, 3)
    path.with_name(path.name+'.cursor').write_text('broken')
    d = append(path, candidate, 4)
    path.with_name(path.name+'.cursor').unlink()
    e = append(path, candidate, 5)
    assert [r['call_seq'] for r in [a,b,c,d,e]] == [1,2,3,4,5]
    assert [r['role'] for r in [a,b,c,d,e]] == ['golden'] + ['candidate']*4


def test_cursor_does_not_reparse_indexed_events(tmp_path, monkeypatch):
    path, candidate = tmp_path/'predicate.jsonl', tmp_path/'input.smt2'
    candidate.write_text('(check-sat)')
    for i in range(12): append(path, candidate, i)
    loads = predicate.json.loads
    parsed = []
    def record(text):
        value = loads(text)
        parsed.append(value)
        return value
    monkeypatch.setattr(predicate.json, 'loads', record)
    append(path, candidate, 13)
    assert not any(isinstance(x, dict) and x.get('event') == 'start' for x in parsed)


def test_concurrent_append_has_one_golden_and_unique_sequences(tmp_path):
    path, candidate = tmp_path/'predicate.jsonl', tmp_path/'input.smt2'
    candidate.write_text('(check-sat)')
    with ThreadPoolExecutor(max_workers=12) as pool:
        rows = list(pool.map(lambda i: append(path, candidate, i), range(40)))
    assert sorted(r['call_seq'] for r in rows) == list(range(1,41))
    assert sum(r['role']=='golden' for r in rows) == 1
    assert len(path.read_text().splitlines()) == 40


def test_cursor_rebuild_after_truncation_and_replacement(tmp_path):
    path, candidate = tmp_path/'predicate.jsonl', tmp_path/'input.smt2'
    candidate.write_text('(check-sat)')
    append(path, candidate, 1)
    path.write_text('')
    assert append(path, candidate, 2)['call_seq'] == 1
    path.unlink()
    assert append(path, candidate, 3)['role'] == 'golden'


def test_corrupt_journal_suffix_is_not_hidden_by_cursor(tmp_path):
    path, candidate = tmp_path/'predicate.jsonl', tmp_path/'input.smt2'
    candidate.write_text('(check-sat)')
    append(path, candidate, 1)
    with path.open('a') as handle: handle.write('{broken\n')
    with pytest.raises(ValueError): append(path, candidate, 2)


def test_cursor_recovers_valid_json_with_damaged_metadata(tmp_path):
    path, candidate = tmp_path/'predicate.jsonl', tmp_path/'input.smt2'
    candidate.write_text('(check-sat)')
    append(path, candidate, 1)
    index = path.with_name(path.name+'.cursor')
    cursor = json.loads(index.read_text())
    cursor['next_call_seq'] = 77
    index.write_text(json.dumps(cursor))
    assert append(path, candidate, 2)['call_seq'] == 2


def test_cursor_recovers_journal_rewritten_in_place(tmp_path):
    path, candidate = tmp_path/'predicate.jsonl', tmp_path/'input.smt2'
    candidate.write_text('(check-sat)')
    row = append(path, candidate, 1)
    original_inode = path.stat().st_ino
    row.update(call_seq=40, call_id='replacement' * 100)
    path.write_text(json.dumps(row)+'\n')
    assert path.stat().st_ino == original_inode
    assert append(path, candidate, 2)['call_seq'] == 41
