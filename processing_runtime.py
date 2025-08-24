"""
processing_runtime.py

Refactored to use agent.py's streaming mechanism for live runs and a lightweight
dummy streamer otherwise. Unnecessary legacy runtime code removed.
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from agent import AuditContext, stream_run


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


# Minimal rule catalog for UI progress and labeling
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


# Expected uploads (filename -> logical table name the agent tools use)
EXPECTED_FILE_TABLE: Dict[str, str] = {
    "journal_entries.csv": "jes",
    "invoices.csv": "invoices",
    "vendors.csv": "vendors",
    "employees.csv": "employees",
    "user_access.csv": "user_access",
}


def validate_and_map_files(files: List[Path]) -> Dict[str, Path]:
    allowed = set(EXPECTED_FILE_TABLE.keys())
    table_to_path: Dict[str, Path] = {}
    for p in files:
        name = p.name
        if name not in allowed:
            raise ValueError(f"Unsupported file '{name}'. Allowed: {sorted(allowed)}")
        table_to_path[EXPECTED_FILE_TABLE[name]] = p
    return table_to_path


# Map UI rule IDs to concrete agent tools (function names defined in agent.py)
RULE_TO_TOOL: Dict[str, str] = {
    "UAR-002": "je_same_user_post_approve",
    "UAR-001": "terminated_users_with_access",
    "TXN-101": "p2p_duplicate_invoices",
    "AUD-007": "fictitious_vendors",
}


async def run_agent(files: List[Path]) -> None:
    try:
        if str(os.getenv("LIVE_AGENT", "")).strip().lower() in {"1", "true", "yes"}:
            await run_agent_live(files)
        else:
            await run_agent_dummy(files)
    except asyncio.CancelledError:
        return
    except Exception as e:
        await emit(Event("rule_failed", data={"error": str(e)}))
        await emit(Event("done"))


async def run_agent_dummy(files: List[Path]) -> None:
    # Validate files only to provide early feedback; not used further here
    _ = validate_and_map_files(files)

    total = len(DUMMY_RULES)
    completed = 0
    total_findings = 0
    await emit(Event("overall", data={"completed": 0, "total": total, "findings": 0}))

    # Simple severity mapping for report metrics
    severity_by_rule = {
        "UAR-001": "critical",
        "UAR-002": "high",
        "ACC-010": "high",
        "PRV-004": "medium",
        "LOG-021": "medium",
        "CFG-002": "medium",
        "TXN-101": "high",
        "AUD-007": "medium",
    }

    audit_findings: List[Dict[str, Any]] = []

    for rule in DUMMY_RULES:
        rid = rule["id"]
        start = time.perf_counter()
        await emit(
            Event("rule_started", rule_id=rid, data={"title": rule["title"], "tag": rule["tag"]})
        )
        await emit(Event("rule_status", rule_id=rid, data={"text": "Initializing datasets"}))
        await asyncio.sleep(0.15)
        await emit(
            Event(
                "tool_call",
                rule_id=rid,
                data={"name": "load_dataset", "args": {"source": "uploaded csvs"}},
            )
        )
        await asyncio.sleep(0.2)
        rows = random.randint(500, 5000)
        await emit(
            Event(
                "tool_result",
                rule_id=rid,
                data={"name": "load_dataset", "ok": True, "summary": f"{rows} rows", "ms": 200},
            )
        )
        await emit(Event("rule_status", rule_id=rid, data={"text": "Scoring anomalies"}))
        await asyncio.sleep(0.2)
        await emit(
            Event(
                "tool_call", rule_id=rid, data={"name": "score_findings", "args": {"top_k": 50}}
            )
        )
        keep = max(0, int(rows * random.uniform(0.01, 0.05)))
        await asyncio.sleep(0.25)
        await emit(
            Event(
                "tool_result",
                rule_id=rid,
                data={"name": "score_findings", "ok": True, "summary": f"{keep} retained", "ms": 250},
            )
        )

        dur_ms = int((time.perf_counter() - start) * 1000)
        severity = severity_by_rule.get(rid, "medium")
        count = max(0, int(keep * random.uniform(0.05, 0.4)))
        audit_findings.append(
            {
                "test": rule["title"],
                "severity": severity,
                "count": count,
                "sample_ids": [],
                "notes": None,
            }
        )
        completed += 1
        total_findings += count
        await emit(Event("rule_completed", rule_id=rid, data={"findings": count, "ms": dur_ms}))
        await emit(
            Event(
                "overall",
                data={"completed": completed, "total": total, "findings": total_findings},
            )
        )
        await asyncio.sleep(0.1)

    # Build report for UI
    total_flags = sum(int(f["count"]) for f in audit_findings)

    def sev_sum(level: str) -> int:
        return sum(int(f["count"]) for f in audit_findings if f["severity"].lower() == level)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": f"{len(audit_findings)} tests run, {total_flags} total flags.",
        "metrics": {
            "rules_total": len(audit_findings),
            "findings": total_flags,
            "critical": sev_sum("critical"),
            "high": sev_sum("high"),
            "medium": sev_sum("medium"),
        },
        "action_items": [
            {
                "title": f"Review {f['test']} ({f['count']} findings)",
                "owner": "You",
                "due": "Today",
            }
            for f in audit_findings
            if int(f.get("count", 0)) > 0
        ],
        "raw": {
            "findings": audit_findings,
            "summary": f"{len(audit_findings)} tests run, {total_flags} total flags.",
        },
    }
    await emit(Event("done", data={"report": report}))


async def run_agent_live(files: List[Path]) -> None:
    table_paths = validate_and_map_files(files)

    checks = [
        "je_same_user_post_approve",
        "p2p_duplicate_invoices",
        "fictitious_vendors",
        "terminated_users_with_access",
    ]
    total = len(checks)
    completed = 0
    total_findings = 0
    await emit(Event("overall", data={"completed": 0, "total": total, "findings": 0}))

    # Helper maps for rule lifecycle
    rule_by_id: Dict[str, Dict[str, Any]] = {r["id"]: r for r in DUMMY_RULES}
    rid_by_tool: Dict[str, str] = {v: k for k, v in RULE_TO_TOOL.items()}
    started_at: Dict[str, float] = {}
    finished: set[str] = set()

    async def start_rule_if_needed(tool_name: str) -> Optional[str]:
        rid = rid_by_tool.get(tool_name)
        if not rid:
            return None
        if rid not in started_at:
            started_at[rid] = time.perf_counter()
            rule = rule_by_id.get(rid, {})
            await emit(
                Event(
                    "rule_started",
                    rule_id=rid,
                    data={"title": rule.get("title", ""), "tag": rule.get("tag", "")},
                )
            )
            await emit(Event("rule_status", rule_id=rid, data={"text": f"LLM: invoking {tool_name}"}))
        return rid

    # Hooks to surface tool lifecycle with names (more precise than stream_run tool items)
    class UIHooks:
        async def on_tool_start(self, context: Any, agent: Any, tool: Any) -> None:
            try:
                name = getattr(tool, "name", "")
                if name == "load_csv":
                    return
                rid = await start_rule_if_needed(name)
                await emit(Event("tool_call", rule_id=rid, data={"name": name, "args": {}}))
            except Exception:
                pass

        async def on_tool_end(self, context: Any, agent: Any, tool: Any, result: str) -> None:
            try:
                name = getattr(tool, "name", "")
                if name == "load_csv":
                    return
                # Try to parse count from Finding JSON; non-finding tools (compile_report) won't have it
                count = 0
                try:
                    import json as _json

                    data = _json.loads(result)
                    if isinstance(data, dict) and "count" in data:
                        count = int(data.get("count") or 0)
                except Exception:
                    pass

                rid = await start_rule_if_needed(name)
                await emit(
                    Event(
                        "tool_result",
                        rule_id=rid,
                        data={"name": name, "ok": True, "summary": f"{count} findings" if count else "done", "ms": 0},
                    )
                )

                if rid and rid not in finished and name in checks:
                    finished.add(rid)
                    nonlocal completed, total_findings
                    completed += 1
                    total_findings += count
                    dur_ms = int((time.perf_counter() - started_at.get(rid, time.perf_counter())) * 1000)
                    await emit(Event("rule_completed", rule_id=rid, data={"findings": count, "ms": dur_ms}))
                    await emit(
                        Event(
                            "overall",
                            data={"completed": completed, "total": total, "findings": total_findings},
                        )
                    )
            except Exception:
                pass

    # Build plan: load CSVs then run checks and compile_report
    load_steps = [
        f"load_csv(name='{table}', path='{str(path)}')" for table, path in table_paths.items()
    ]
    plan = (
        "1) "
        + "\n2) ".join(load_steps)
        + "\nThen run "
        + ", ".join(checks)
        + ", and compile_report to produce a single JSON AuditReport."
    )

    ctx = AuditContext()

    # Consume the agent's event stream; transform to UI events for reasoning/status and done
    async for ev in stream_run(plan, context=ctx, hooks=UIHooks()):
        et = ev.get("type")
        if et == "reasoning":
            text = (ev.get("text") or "").strip()
            if text:
                await emit(Event("rule_status", data={"text": f"LLM: {text}"}))
        elif et == "assistant_message":
            preview = (ev.get("text_preview") or "").strip()
            if preview:
                await emit(Event("rule_status", data={"text": f"LLM: {preview}"}))
        elif et == "done":
            # Try to convert final_output JSON -> UI report shape
            report_payload: Optional[Dict[str, Any]] = None
            try:
                import json as _json

                final = ev.get("final_output")
                data = _json.loads(final) if isinstance(final, str) else final
                if isinstance(data, dict):
                    findings = data.get("findings") or []
                    total_flags = sum(int(f.get("count", 0)) for f in findings if isinstance(f, dict))

                    def _sev(level: str) -> int:
                        return sum(
                            int(f.get("count", 0))
                            for f in findings
                            if isinstance(f, dict) and str(f.get("severity", "")).lower() == level
                        )

                    report_payload = {
                        "generated_at": datetime.now().isoformat(timespec="seconds"),
                        "summary": data.get("summary", ""),
                        "metrics": {
                            "rules_total": len(findings),
                            "findings": total_flags,
                            "critical": _sev("critical"),
                            "high": _sev("high"),
                            "medium": _sev("medium"),
                        },
                        "action_items": [
                            {
                                "title": f"Review {f.get('test')} ({int(f.get('count', 0))} findings)",
                                "owner": "You",
                                "due": "Today",
                            }
                            for f in findings
                            if isinstance(f, dict) and int(f.get("count", 0)) > 0
                        ],
                        "raw": data,
                    }
            except Exception:
                report_payload = None

            if report_payload:
                await emit(Event("done", data={"report": report_payload}))
            else:
                await emit(Event("done"))

