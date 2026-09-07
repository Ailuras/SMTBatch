"""Real TERM/INT delivery at journal and Popen boundaries must retain closure."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest

from smtbatch import predicate


@pytest.mark.parametrize('boundary', ['after_start', 'after_spawn', 'before_finish'])
@pytest.mark.parametrize('signum', [signal.SIGTERM, signal.SIGINT])
def test_signal_at_predicate_lifecycle_boundary(tmp_path, boundary, signum):
    driver = tmp_path/'driver.py'
    driver.write_text('''import os, signal, sys
from pathlib import Path
from smtbatch import predicate
work=Path(sys.argv[1]);boundary=sys.argv[2];signum=int(sys.argv[3])
previous={s:signal.getsignal(s) for s in (signal.SIGTERM,signal.SIGINT)}
original_start=predicate._append_start
original_popen=predicate.subprocess.Popen
original_decode=predicate.decode
def start(*args,**kwargs):
    result=original_start(*args,**kwargs)
    if boundary=='after_start':os.kill(os.getpid(),signum)
    return result
def spawn(*args,**kwargs):
    result=original_popen(*args,**kwargs)
    (work/'child.pid').write_text(str(result.pid))
    if boundary=='after_spawn':os.kill(os.getpid(),signum)
    return result
def decode(*args,**kwargs):
    if boundary=='before_finish':os.kill(os.getpid(),signum)
    return original_decode(*args,**kwargs)
predicate._append_start=start
predicate.subprocess.Popen=spawn
predicate.decode=decode
command=[sys.executable,'-c','import time;time.sleep(30)' if boundary=='after_spawn' else 'pass']
code=predicate.main(['--log',str(work/'journal.jsonl'),'--phase','reducer',
    '--solver-timeout','40','--',*command,str(work/'input.smt2')])
assert all(signal.getsignal(s)==handler for s,handler in previous.items())
raise SystemExit(code)
''')
    (tmp_path/'input.smt2').write_text('(check-sat)\n')
    env=dict(os.environ, PYTHONPATH=str(Path(predicate.__file__).resolve().parents[1]))
    child_pid=None
    try:
        result=subprocess.run([sys.executable,str(driver),str(tmp_path),boundary,str(int(signum))],
                              capture_output=True,text=True,env=env,timeout=8)
        if (tmp_path/'child.pid').exists():child_pid=int((tmp_path/'child.pid').read_text())
        assert result.returncode==128+signum, result.stderr
        rows=[json.loads(l) for l in (tmp_path/'journal.jsonl').read_text().splitlines()]
        assert [r['event'] for r in rows]==['start','finish']
        assert rows[0]['call_id']==rows[1]['call_id']
        assert rows[1]['killed'] and not rows[1]['timed_out']
        assert rows[1]['returncode']==128+signum
        if boundary=='after_start':assert child_pid is None
        if child_pid is not None:
            with pytest.raises(ProcessLookupError):os.kill(child_pid,0)
    finally:
        if child_pid is not None:
            try:os.killpg(child_pid,signal.SIGKILL)
            except ProcessLookupError:pass
