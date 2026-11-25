"""What the DAG tests ask about the DAGs, answered from either place Airflow can be reached.

Airflow is not installed in the dev venv; it runs in the sibling `airflow` container, whose REST API
(AIRFLOW_URL, AIRFLOW_AUTH from mise.toml) lists the same DAGs. `load()` uses a local DagBag when Airflow
is importable and the REST API otherwise, and skips with the reason when neither is reachable. The REST
view reflects the DAG folder as the container last parsed it (it rescans every ~30 s), so wait that long
after editing a DAG file.
"""
from __future__ import annotations

import base64
import importlib.util
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import FrozenSet, List, Mapping, Optional

import pytest

DAGS = os.path.join(os.path.dirname(__file__), "..", "dags")
_TIMEOUT_S = 5


@dataclass(frozen=True)
class TaskView:
    retries: int
    trigger_rule: str
    upstream: FrozenSet[str]


@dataclass(frozen=True)
class DagView:
    max_active_runs: int
    catchup: bool
    is_paused_upon_creation: bool
    schedule: Optional[str]          # a cron expression, or None for a DAG that is only triggered
    tasks: Mapping[str, TaskView]

    def task_order(self) -> List[str]:
        """Task ids in dependency order (ties broken by id)."""
        done: List[str] = []
        while len(done) < len(self.tasks):
            ready = sorted(t for t, v in self.tasks.items() if t not in done and v.upstream <= set(done))
            if not ready:
                raise ValueError("task dependencies form a cycle")
            done.append(ready[0])
        return done


@dataclass(frozen=True)
class Catalog:
    import_errors: Mapping[str, str]
    dags: Mapping[str, DagView]


def _from_dagbag() -> Catalog:
    from airflow.models import DagBag

    bag = DagBag(DAGS, include_examples=False)
    dags = {}
    for dag_id, dag in bag.dags.items():
        tasks = {t.task_id: TaskView(int(t.retries), str(t.trigger_rule), frozenset(t.upstream_task_ids))
                 for t in dag.tasks}
        dags[dag_id] = DagView(dag.max_active_runs, bool(dag.catchup), bool(dag.is_paused_upon_creation),
                               None if str(dag.timetable.summary) == "None" else str(dag.timetable.summary), tasks)
    return Catalog(dict(bag.import_errors), dags)


class _Rest:
    def __init__(self, url: str, auth: str):
        self._url = url.rstrip("/") + "/api/v1"
        self._header = "Basic " + base64.b64encode(auth.encode("utf-8")).decode("ascii")

    def get(self, path: str):
        req = urllib.request.Request(self._url + path, headers={"Authorization": self._header})
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            return json.load(resp)


def _from_rest(url: str, auth: str) -> Catalog:
    rest = _Rest(url, auth)
    import_errors = {e["filename"]: e["stack_trace"] for e in rest.get("/importErrors")["import_errors"]}
    dags = {}
    for entry in rest.get("/dags?limit=100")["dags"]:
        dag_id = entry["dag_id"]
        detail = rest.get("/dags/%s/details" % dag_id)
        listed = rest.get("/dags/%s/tasks" % dag_id)["tasks"]
        downstream = {t["task_id"]: set(t["downstream_task_ids"]) for t in listed}
        tasks = {t["task_id"]: TaskView(int(t["retries"]), t["trigger_rule"],
                                        frozenset(u for u, d in downstream.items() if t["task_id"] in d))
                 for t in listed}
        interval = detail["schedule_interval"]
        dags[dag_id] = DagView(detail["max_active_runs"], detail["catchup"], detail["is_paused_upon_creation"],
                               interval["value"] if interval else None, tasks)
    return Catalog(import_errors, dags)


def load() -> Catalog:
    if importlib.util.find_spec("airflow") is not None:
        return _from_dagbag()
    url, auth = os.environ.get("AIRFLOW_URL"), os.environ.get("AIRFLOW_AUTH")
    if not url or not auth:
        pytest.skip("DAG tests not run: Airflow is not installed and AIRFLOW_URL/AIRFLOW_AUTH are unset "
                    "(run through `mise run`)")
    try:
        return _from_rest(url, auth)
    except (urllib.error.URLError, OSError) as exc:
        pytest.skip("DAG tests not run: Airflow is not installed and %s is unreachable (%s)" % (url, exc))
