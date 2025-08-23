# processing_runtime.py
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

EventType = Literal[
    "overall",
    "rule_started",
    "rule_status",
    "rule_progress",
    "tool_call",
    "tool_result",
    "rule_completed",
    "rule_failed",
    "done",
]


@dataclass
class Event:
    type: EventType
    rule_id: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


EVENT_BUS: asyncio.Queue[Event] = asyncio.Queue()


async def emit(ev: Event) -> None:
    await EVENT_BUS.put(ev)


# Demo rule catalog you can tweak
DUMMY_RULES: List[Dict[str, Any]] = [
    {"id": "UAR-001", "title": "Terminated User Access Testing", "tag": "Fraud"},
    {"id": "UAR-002", "title": "Segregation of Duties", "tag": "Fraud"},
    {"id": "ACC-010", "title": "Excessive Privilege Escalations", "tag": "Access"},
    {"id": "PRV-004", "title": "Stale Admin Accounts", "tag": "Access"},
    {"id": "LOG-021", "title": "Suspicious Login Bursts", "tag": "Security"},
    {"id": "CFG-002", "title": "Weak MFA Enrollment", "tag": "Config"},
    {"id": "TXN-101", "title": "Unusual High-Value Transfers", "tag": "Fraud"},
    {"id": "AUD-007", "title": "Missing Evidence Attachments", "tag": "Audit"},
]


async def run_engine(files: List[Path]) -> None:
    try:
        total = len(DUMMY_RULES)
        completed = 0
        total_findings = 0
        await emit(
            Event(
                "overall",
                data={
                    "completed": completed,
                    "total": total,
                    "findings": total_findings,
                },
            )
        )

        for rule in DUMMY_RULES:
            start_ms = time.perf_counter()
            rid = rule["id"]

            await emit(
                Event(
                    "rule_started",
                    rule_id=rid,
                    data={"title": rule["title"], "tag": rule["tag"]},
                )
            )
            await emit(Event("rule_progress", rule_id=rid, data={"pct": 0.02}))
            await emit(
                Event(
                    "rule_status", rule_id=rid, data={"text": "Initializing datasets"}
                )
            )
            await asyncio.sleep(0.2)

            await emit(
                Event(
                    "tool_call",
                    rule_id=rid,
                    data={"name": "load_dataset", "args": {"source": "users, roles"}},
                )
            )
            rows = random.randint(500, 5000)
            await asyncio.sleep(0.25)
            await emit(
                Event(
                    "tool_result",
                    rule_id=rid,
                    data={
                        "name": "load_dataset",
                        "ok": True,
                        "summary": f"{rows} rows",
                        "ms": 250,
                    },
                )
            )
            await emit(Event("rule_progress", rule_id=rid, data={"pct": 0.25}))
            await emit(
                Event(
                    "rule_status",
                    rule_id=rid,
                    data={"text": "Joining role grants with terminations"},
                )
            )
            await asyncio.sleep(0.2)

            await emit(
                Event(
                    "tool_call",
                    rule_id=rid,
                    data={"name": "compute_candidates", "args": {"window_days": 90}},
                )
            )
            cands = max(1, int(rows * random.uniform(0.01, 0.05)))
            await asyncio.sleep(0.25)
            await emit(
                Event(
                    "tool_result",
                    rule_id=rid,
                    data={
                        "name": "compute_candidates",
                        "ok": True,
                        "summary": f"{cands} candidates",
                        "ms": 250,
                    },
                )
            )
            await emit(Event("rule_progress", rule_id=rid, data={"pct": 0.55}))
            await emit(
                Event("rule_status", rule_id=rid, data={"text": "Scoring anomalies"})
            )
            await asyncio.sleep(0.2)

            await emit(
                Event(
                    "tool_call",
                    rule_id=rid,
                    data={
                        "name": "score_findings",
                        "args": {"model": "o4-mini", "top_k": 50},
                    },
                )
            )
            keep = max(0, int(cands * random.uniform(0.05, 0.35)))
            await asyncio.sleep(0.3)
            await emit(
                Event(
                    "tool_result",
                    rule_id=rid,
                    data={
                        "name": "score_findings",
                        "ok": True,
                        "summary": f"{keep} retained",
                        "ms": 300,
                    },
                )
            )
            await emit(Event("rule_progress", rule_id=rid, data={"pct": 0.85}))
            await emit(
                Event("rule_status", rule_id=rid, data={"text": "Finalizing outputs"})
            )
            await asyncio.sleep(0.2)

            findings = keep
            completed += 1
            total_findings += findings
            dur_ms = int((time.perf_counter() - start_ms) * 1000)
            await emit(
                Event(
                    "rule_completed",
                    rule_id=rid,
                    data={"findings": findings, "ms": dur_ms},
                )
            )
            await emit(
                Event(
                    "overall",
                    data={
                        "completed": completed,
                        "total": total,
                        "findings": total_findings,
                    },
                )
            )

            await asyncio.sleep(0.15)

        await emit(Event("done"))
    except asyncio.CancelledError:
        # Swallow cancellation cleanly when user navigates away
        return
