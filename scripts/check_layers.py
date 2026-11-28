"""Fail when a module imports from a higher layer, or sits in no layer.

The rule is in `.claude/rules/design-principles.md`: dependencies point toward a core that does no I/O. A module may
import from its own layer or a lower one. Existing violations are listed in BASELINE; the baseline only shrinks. When
you fix one, remove its entry (the check tells you to), so the gain cannot be spent again.

To move a module to another layer, edit its line in LAYER_MAP.

Usage: python scripts/check_layers.py   (run by `mise run lint`)
"""
from __future__ import annotations

import ast
import os
import sys
from typing import FrozenSet, List, Mapping, Optional, Sequence, Set, Tuple

ROOTS = ("citibike_contract", "citibike_pipeline", "citibike_producer", "dags")

# chain -> its layers, lowest first. The contract is shared: any chain may import it. The DAGs sit above both packages.
CHAINS = {
    "contract": ["contract"],
    "pipeline": ["core", "infrastructure", "orchestration and entry"],
    "producer": ["sources and sink", "jobs", "task entry points"],
    "dags": ["orchestration and entry"],
}
SHARED_CHAIN = "contract"
TOP_CHAIN = "dags"

# module -> "chain:layer". Every module under ROOTS must be here.
LAYER_MAP = {
    "citibike_contract": "contract:contract",
    "citibike_contract.envelope": "contract:contract",
    "citibike_contract.poll_log": "contract:contract",
    "citibike_contract.invalid_envelope": "contract:contract",
    "citibike_contract.missing_kafka_key": "contract:contract",
    # pipeline: core (pure values and logic)
    "citibike_pipeline": "pipeline:core",
    "citibike_pipeline.config": "pipeline:core",
    "citibike_pipeline.enrichment": "pipeline:core",
    "citibike_pipeline.jsonfields": "pipeline:core",
    "citibike_pipeline.obs": "pipeline:core",
    "citibike_pipeline.percentile": "pipeline:core",
    "citibike_pipeline.rows": "pipeline:core",
    "citibike_pipeline.snapshot": "pipeline:core",
    "citibike_pipeline.station_state": "pipeline:core",
    "citibike_pipeline.station_window": "pipeline:core",
    "citibike_pipeline.timeutil": "pipeline:core",
    "citibike_pipeline.weather_reading": "pipeline:core",
    "citibike_pipeline.station_information": "pipeline:core",
    "citibike_pipeline.window_metrics": "pipeline:core",
    # pipeline: infrastructure (Kafka, storage, the lake)
    "citibike_pipeline.batch_log": "pipeline:infrastructure",
    "citibike_pipeline.batch_log_entry": "pipeline:infrastructure",
    "citibike_pipeline.counts": "pipeline:infrastructure",
    "citibike_pipeline.plan": "pipeline:infrastructure",
    "citibike_pipeline.stats": "pipeline:infrastructure",
    "citibike_pipeline.latencies": "pipeline:infrastructure",
    "citibike_pipeline.dead_letter_row": "pipeline:infrastructure",
    "citibike_pipeline.decoded_batch": "pipeline:infrastructure",
    "citibike_pipeline.enrichment_context": "pipeline:infrastructure",
    "citibike_pipeline.lake": "pipeline:infrastructure",
    "citibike_pipeline.raw_row": "pipeline:infrastructure",
    "citibike_pipeline.schemas": "pipeline:infrastructure",
    "citibike_pipeline.serving": "pipeline:infrastructure",
    "citibike_pipeline.storage": "pipeline:infrastructure",
    "citibike_pipeline.s3_paginator": "pipeline:infrastructure",
    "citibike_pipeline.s3_exceptions": "pipeline:infrastructure",
    "citibike_pipeline.s3_client": "pipeline:infrastructure",
    "citibike_pipeline.s3_body": "pipeline:infrastructure",
    # pipeline: orchestration and entry
    "citibike_pipeline.batch": "pipeline:orchestration and entry",
    "citibike_pipeline.quality": "pipeline:orchestration and entry",
    "citibike_pipeline.poll_gap": "pipeline:core",
    "citibike_pipeline.empty_station": "pipeline:core",
    "citibike_pipeline.batch_latency": "pipeline:core",
    "citibike_pipeline.daily_quality_report_thresholds": "pipeline:core",
    "citibike_pipeline.poll_section": "pipeline:infrastructure",
    "citibike_pipeline.section_status": "pipeline:infrastructure",
    "citibike_pipeline.batch_section": "pipeline:infrastructure",
    "citibike_pipeline.dlq_section": "pipeline:infrastructure",
    "citibike_pipeline.verdict": "pipeline:infrastructure",
    "citibike_pipeline.daily_quality_report": "pipeline:infrastructure",
    "citibike_pipeline.runtime": "pipeline:orchestration and entry",
    "citibike_pipeline.station_info": "pipeline:orchestration and entry",
    "citibike_pipeline.dim_station_build": "pipeline:infrastructure",
    "citibike_pipeline.dim_station_row": "pipeline:infrastructure",
    "citibike_pipeline.station_information_table": "pipeline:infrastructure",
    "citibike_pipeline.status": "pipeline:orchestration and entry",
    "citibike_pipeline.rejected": "pipeline:core",
    "citibike_pipeline.jsonnumbers": "pipeline:core",
    "citibike_pipeline.station_status": "pipeline:core",
    "citibike_pipeline.weather_observation": "pipeline:core",
    "citibike_pipeline.hour_partitions": "pipeline:core",
    "citibike_pipeline.delivered": "pipeline:core",
    "citibike_pipeline.record": "pipeline:core",
    "citibike_pipeline.ranges": "pipeline:core",
    "citibike_pipeline.kafka_batch_reader": "pipeline:infrastructure",
    "citibike_pipeline.lake_table": "pipeline:infrastructure",
    "citibike_pipeline.compact_hour": "pipeline:infrastructure",
    "citibike_pipeline.local_storage": "pipeline:infrastructure",
    "citibike_pipeline.s3_storage": "pipeline:infrastructure",
    "citibike_pipeline.storage_ops": "pipeline:infrastructure",
    # producer
    "citibike_producer": "producer:sources and sink",
    "citibike_producer.config": "producer:sources and sink",
    "citibike_producer.create_topics": "producer:sources and sink",
    "citibike_producer.discovery_state": "producer:sources and sink",
    "citibike_producer.jsonvalues": "producer:sources and sink",
    "citibike_producer.producer_like": "producer:sources and sink",
    "citibike_producer.station_feed": "producer:sources and sink",
    "citibike_producer.job_state": "producer:jobs",
    "citibike_producer.replay": "producer:jobs",
    "citibike_producer.broker": "producer:task entry points",
    "citibike_producer.synthetic": "producer:task entry points",
    "citibike_producer.poll_state": "producer:task entry points",
    "citibike_producer.task": "producer:task entry points",
    "citibike_producer.task_instance_like": "producer:task entry points",
    "citibike_producer.tasks": "producer:task entry points",
    "citibike_producer.fetch_result": "producer:sources and sink",
    "citibike_producer.http_client": "producer:sources and sink",
    "citibike_producer.feed_registry": "producer:sources and sink",
    "citibike_producer.weather_source": "producer:sources and sink",
    "citibike_producer.delivery_batch": "producer:sources and sink",
    "citibike_producer.kafka_sink": "producer:sources and sink",
    "citibike_producer.poll": "producer:jobs",
    "citibike_producer.poll_source": "producer:jobs",
    "citibike_producer.feed_read": "producer:jobs",
    "citibike_producer.unreadable_feed": "producer:jobs",
    "citibike_producer.poll_outcome": "producer:jobs",
    "citibike_producer.poll_run": "producer:jobs",
    "citibike_producer.poll_delivery": "producer:jobs",
    "citibike_producer.gbfs_feed": "producer:jobs",
    "citibike_producer.weather_feed": "producer:jobs",
    # DAGs
    "dags._common": "dags:orchestration and entry",
    "dags.citibike_compaction": "dags:orchestration and entry",
    "dags.citibike_daily_quality": "dags:orchestration and entry",
    "dags.citibike_ingest_status": "dags:orchestration and entry",
    "dags.citibike_ingest_weather": "dags:orchestration and entry",
    "dags.citibike_micro_batch": "dags:orchestration and entry",
    "dags.citibike_replay": "dags:orchestration and entry",
    "dags.citibike_setup": "dags:orchestration and entry",
    "dags.citibike_station_info": "dags:orchestration and entry",
}

# (importing module, imported module) pairs that import from a higher layer today. Only ever remove entries.
BASELINE: FrozenSet[Tuple[str, str]] = frozenset({
    ("citibike_pipeline.batch_log_entry", "citibike_pipeline.batch"),    # Counts: candidate 5
    ("citibike_pipeline.enrichment", "citibike_pipeline.batch_log"),    # Counts: candidate 5
})


def _module_name(root: str, path: str) -> str:
    parts = os.path.relpath(path, root)[: -len(".py")].split(os.sep)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _modules(root: str, roots: Sequence[str]) -> Mapping[str, str]:
    """Module name -> file path for every module under the roots."""
    found = {}
    for top in roots:  # noqa: PLR1702
        for here, _dirs, files in os.walk(os.path.join(root, top)):
            for name in sorted(files):
                if name.endswith(".py"):
                    path = os.path.join(here, name)
                    found[_module_name(root, path)] = path
    return found


def _absolute(importer: str, is_package: bool, node: ast.ImportFrom) -> Optional[str]:
    """The module a `from ... import` names, with relative levels resolved."""
    if node.level == 0:
        return node.module
    base = importer.split(".")
    if not is_package:
        base = base[:-1]
    base = base[: len(base) - (node.level - 1)]
    if node.module:
        base = base + node.module.split(".")
    return ".".join(base) or None


def _resolve(name: str, known: Set[str]) -> Optional[str]:
    if name in known:
        return name
    if "dags." + name in known:        # Airflow puts dags/ on sys.path, so DAGs import `_common` bare
        return "dags." + name
    return None


def _imports(importer: str, path: str, known: Set[str]) -> List[str]:
    with open(path, encoding="utf-8") as fh:
        tree = ast.parse(fh.read(), filename=path)
    is_package = os.path.basename(path) == "__init__.py"
    found: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            targets = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = _absolute(importer, is_package, node)
            if base is None:
                continue
            targets = [base] + [base + "." + alias.name for alias in node.names]
        else:
            continue
        for target in targets:
            resolved = _resolve(target, known)
            if resolved is not None and resolved != importer:
                found.append(resolved)
    # `from pkg import mod` names both pkg and pkg.mod: keep the most specific module of each import
    return sorted({m for m in found if not any(o.startswith(m + ".") and o in found for o in found)})


def _layer(entry: str) -> Tuple[str, str]:
    chain, _, name = entry.partition(":")
    return chain, name


def _is_higher(importer: Tuple[str, str], imported: Tuple[str, str], chains: Mapping[str, Sequence[str]]) -> bool:
    # Within a chain, higher means later in its list. Across chains, only the shared contract may be imported,
    # except by the DAGs, which sit above both packages.
    if importer[0] == imported[0]:
        order = chains[importer[0]]
        return order.index(imported[1]) > order.index(importer[1])
    return imported[0] != SHARED_CHAIN and importer[0] != TOP_CHAIN


def _describe(entry: str) -> str:
    chain, name = _layer(entry)
    return name if chain == SHARED_CHAIN else chain + ": " + name


def main(
    root: str = ".",
    layer_map: Mapping[str, str] = LAYER_MAP,
    baseline: FrozenSet[Tuple[str, str]] = BASELINE,
    chains: Mapping[str, Sequence[str]] = CHAINS,
    roots: Sequence[str] = ROOTS,
) -> int:
    modules = _modules(root, roots)
    known = set(modules)
    problems: List[str] = []
    seen: Set[Tuple[str, str]] = set()
    for importer in sorted(modules):
        if importer not in layer_map:
            problems.append("%s is in no layer: add it to LAYER_MAP in scripts/check_layers.py" % importer)
            continue
        for imported in _imports(importer, modules[importer], known):
            if imported not in layer_map or not _is_higher(
                _layer(layer_map[importer]), _layer(layer_map[imported]), chains
            ):
                continue
            pair = (importer, imported)
            seen.add(pair)
            if pair not in baseline:
                problems.append("%s (%s) imports %s (%s), a higher layer" % (
                    importer, _describe(layer_map[importer]), imported, _describe(layer_map[imported])))
    for importer, imported in sorted(baseline - seen):
        problems.append("%s no longer imports %s from a higher layer: remove (%r, %r) from BASELINE in "
                        "scripts/check_layers.py" % (importer, imported, importer, imported))
    for line in problems:
        print(line)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
