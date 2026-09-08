"""Exercise the real dispatcher with short in-process trials, without solvers."""

import csv
import json
import signal
import threading
from collections import Counter

import pytest

from smtbatch import reduce


@pytest.fixture
def study(tmp_path, monkeypatch):
    def make(count=9, workers=2):
        plan = {
            "format": reduce.FORMAT,
            "study_id": "queue-test",
            "execution": {"outer_jobs": workers, "schedule": reduce.SCHEDULE},
            "limits": {"termination_grace_sec": 0.1},
            "reducers": [{"id": "a", "label": "A"}, {"id": "b", "label": "B"}],
            "jobs": [
                {
                    "job_id": str(i), "order": i + 1,
                    "benchmark_id": f"case-{i % 3}",
                    "reducer_id": "ab"[(i // 3) % 2],
                    "repeat": i // 6 + 1,
                }
                for i in range(count)
            ],
        }
        monkeypatch.setattr(reduce, "load_plan", lambda output: plan)
        return tmp_path, plan

    return make


def seal(output, plan, job):
    job_dir = output / "jobs" / job["job_id"]
    attempt, attempt_dir = reduce._next_attempt(job_dir)
    result = {
        **job, "format": reduce.FORMAT, "attempt": attempt,
        "status": "completed", "verified": True, "evidence_ok": True,
    }
    reduce._seal_job(job_dir, attempt_dir, result)
    return result


def progress(output):
    return json.loads((output / "progress.json").read_text())


def test_refills_across_reducers_and_repeats_with_a_slow_first_trial(study, monkeypatch):
    output, plan = study(count=12)
    later_repeat_started = threading.Event()
    refill_observed = []
    started = []

    def execute(output, plan, job):
        started.append(job["job_id"])
        if job["job_id"] == "0":
            refill_observed.append(later_repeat_started.wait(5))
        if job["repeat"] == 2:
            later_repeat_started.set()
        return seal(output, plan, job)

    monkeypatch.setattr(reduce, "_execute_job", execute)
    results = reduce._run_locked(output, plan)
    assert refill_observed == [True]
    assert Counter(started) == Counter(job["job_id"] for job in plan["jobs"])
    assert len(results) == 12
    assert progress(output)["status"] == "complete"


def test_128_workers_fill_slots_without_oversubscription(study, monkeypatch):
    output, plan = study(count=385, workers=128)
    first_workers = threading.Barrier(128, timeout=10)
    next_round = threading.Event()
    lock = threading.Lock()
    active = peak = 0
    refill_observed = []
    started = []

    def execute(output, plan, job):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            started.append(job["job_id"])
        try:
            if int(job["job_id"]) < 128:
                first_workers.wait()
            else:
                next_round.set()
            if job["job_id"] == "0":
                refill_observed.append(next_round.wait(10))
            return seal(output, plan, job)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(reduce, "_execute_job", execute)
    results = reduce._run_locked(output, plan)
    assert peak == 128
    assert active == 0
    assert refill_observed == [True]
    assert len(results) == 385
    assert Counter(started) == Counter(job["job_id"] for job in plan["jobs"])
    assert progress(output)["running_jobs"] == 0
    assert progress(output)["pending_jobs"] == 0


def test_fifo_resume_skips_sealed_trials_and_retries_partial_attempts(study, monkeypatch):
    output, plan = study(count=6, workers=1)
    completed = seal(output, plan, plan["jobs"][2])
    partial = output / "jobs" / "4" / "attempts" / "0001"
    partial.mkdir(parents=True)
    (partial / "partial.json").write_text('{"status":"aborted"}')
    started = []

    def execute(output, plan, job):
        started.append(job["job_id"])
        return seal(output, plan, job)

    monkeypatch.setattr(reduce, "_execute_job", execute)
    results = reduce._run_locked(output, plan)
    assert started == ["0", "1", "3", "4", "5"]
    assert next(item for item in results if item["job_id"] == "2") == completed
    assert next(item for item in results if item["job_id"] == "4")["attempt"] == 2
    assert (partial / "partial.json").is_file()
    with (output / "results.tsv").open() as handle:
        assert [row["job_id"] for row in csv.DictReader(handle, delimiter="\t")] == [str(i) for i in range(6)]


@pytest.mark.parametrize("mode", ["pause", "graceful"])
def test_stop_drains_active_trial_and_resume_continues_queue(study, monkeypatch, mode):
    output, plan = study(count=5, workers=1)
    started = []

    def execute(output, plan, job):
        started.append(job["job_id"])
        if job["job_id"] == "0":
            reduce.request_stop(output, mode)
        return seal(output, plan, job)

    monkeypatch.setattr(reduce, "_execute_job", execute)
    assert len(reduce._run_locked(output, plan)) == 1
    assert started == ["0"]
    assert progress(output)["status"] == "interrupted"
    assert progress(output)["running_jobs"] == 0
    assert progress(output)["pending_jobs"] == 4
    assert len(reduce._run_locked(output, plan)) == 5
    assert started == [str(i) for i in range(5)]
    assert progress(output)["status"] == "complete"


def test_live_pause_and_resume_refills_the_same_queue(study, monkeypatch):
    output, plan = study(count=6)
    ready = threading.Barrier(2, timeout=5)
    paused = threading.Event()
    resumed = threading.Event()
    started = []
    real_progress = reduce._progress

    def record_progress(output, plan, status, *args, **kwargs):
        real_progress(output, plan, status, *args, **kwargs)
        if status == "paused":
            paused.set()

    def execute(output, plan, job):
        started.append(job["job_id"])
        if int(job["job_id"]) < 2:
            ready.wait()
            if job["job_id"] == "0":
                reduce.request_stop(output, "pause")
            assert paused.wait(5)
            if job["job_id"] == "1":
                assert len(started) == 2
                resumed.set()
                reduce.clear_control_for_resume(output)
            else:
                assert resumed.wait(5)
        else:
            assert resumed.is_set()
        return seal(output, plan, job)

    monkeypatch.setattr(reduce, "_execute_job", execute)
    monkeypatch.setattr(reduce, "_progress", record_progress)
    assert len(reduce._run_locked(output, plan)) == 6
    assert progress(output)["status"] == "complete"


def test_immediate_stop_terminates_workers_without_sealing_trials(study, monkeypatch):
    output, plan = study(count=6)
    ready = threading.Barrier(2, timeout=5)
    terminated = threading.Event()
    started = []

    def execute(output, plan, job):
        started.append(job["job_id"])
        ready.wait()
        if job["job_id"] == "0":
            reduce.request_stop(output, "immediate")
        assert terminated.wait(5)
        raise reduce.ImmediateAbort("test interruption")

    monkeypatch.setattr(reduce, "_execute_job", execute)
    monkeypatch.setattr(reduce.PROCESS_REGISTRY, "terminate_all", lambda grace: terminated.set())
    assert reduce._run_locked(output, plan) == []
    assert set(started) == {"0", "1"}
    assert progress(output)["status"] == "interrupted"
    assert progress(output)["running_jobs"] == 0
    assert progress(output)["pending_jobs"] == 6


def test_worker_failure_aborts_peers_before_waiting_for_executor(study, monkeypatch):
    output, plan = study(count=6)
    ready = threading.Barrier(2, timeout=5)
    terminated = threading.Event()
    cleanup_observed = []
    started = []
    handlers = [signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)]

    def execute(output, plan, job):
        started.append(job["job_id"])
        ready.wait()
        if job["job_id"] == "0":
            raise RuntimeError("test worker failure")
        cleanup_observed.append(terminated.wait(5))
        assert reduce._control_mode(output) == "immediate"
        raise reduce.ImmediateAbort("peer failed")

    monkeypatch.setattr(reduce, "_execute_job", execute)
    monkeypatch.setattr(reduce.PROCESS_REGISTRY, "terminate_all", lambda grace: terminated.set())
    with pytest.raises(RuntimeError, match="test worker failure"):
        reduce._run_locked(output, plan)
    assert cleanup_observed == [True]
    assert set(started) == {"0", "1"}
    assert progress(output)["status"] == "failed"
    assert progress(output)["running_jobs"] == 0
    assert handlers == [signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)]


def test_scheduler_scans_sealed_results_once_and_batches_index_updates(study, monkeypatch):
    output, plan = study(count=60, workers=4)
    scans = writes = 0
    real_completed = reduce.completed_results
    real_index = reduce.write_results_index

    def read_results(*args, **kwargs):
        nonlocal scans
        scans += 1
        return real_completed(*args, **kwargs)

    def write_index(*args, **kwargs):
        nonlocal writes
        writes += 1
        assert kwargs.get("results") is not None
        return real_index(*args, **kwargs)

    monkeypatch.setattr(reduce, "_execute_job", seal)
    monkeypatch.setattr(reduce, "completed_results", read_results)
    monkeypatch.setattr(reduce, "write_results_index", write_index)
    assert len(reduce._run_locked(output, plan)) == 60
    assert scans == 1
    assert writes < 10
    assert len(real_completed(output, plan)) == 60


def test_stop_during_refill_does_not_dispatch_remaining_slots(study, monkeypatch):
    output, plan = study(count=300, workers=128)
    executor_class = reduce.concurrent.futures.ThreadPoolExecutor
    submitted = []

    class StopAfterSubmission(executor_class):
        def submit(self, fn, *args, **kwargs):
            future = super().submit(fn, *args, **kwargs)
            submitted.append(args[-1]["job_id"])
            reduce.request_stop(output, "graceful")
            return future

    monkeypatch.setattr(reduce, "_execute_job", seal)
    monkeypatch.setattr(reduce.concurrent.futures, "ThreadPoolExecutor", StopAfterSubmission)
    assert len(reduce._run_locked(output, plan)) == 1
    assert submitted == ["0"]
    assert progress(output)["status"] == "interrupted"
    assert progress(output)["pending_jobs"] == 299


def test_resuming_finished_queue_launches_nothing(study, monkeypatch):
    output, plan = study(count=2)
    for job in plan["jobs"]:
        seal(output, plan, job)

    def unexpected(*args, **kwargs):
        pytest.fail("a sealed job was dispatched again")

    monkeypatch.setattr(reduce, "_execute_job", unexpected)
    assert len(reduce._run_locked(output, plan)) == 2
    assert progress(output)["status"] == "complete"
    assert progress(output)["pending_jobs"] == 0
