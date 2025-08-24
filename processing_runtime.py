# processing_runtime.py
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
import pandas as pd
from datetime import datetime
from agent import (
    compute_je_same_user_post_approve,
    compute_p2p_duplicate_invoices,
    compute_fictitious_vendors,
    compute_terminated_users_with_access,
    Finding,
)

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

# ---- Step 1: file validation + rule-to-tool mapping (no behavior change yet) ----

# Expected uploads (filename -> logical table name used by agent tools)
EXPECTED_FILE_TABLE: Dict[str, str] = {
    "journal_entries.csv": "jes",
    "invoices.csv": "invoices",
    "vendors.csv": "vendors",
    "employees.csv": "employees",
    "user_access.csv": "user_access",
}


def validate_and_map_files(files: List[Path]) -> Dict[str, Path]:
    """Validate uploaded files and map to logical table names.

    - Only known filenames are allowed; raise on unknown names.
    - Returns a mapping: table_name -> file_path
    """
    table_to_path: Dict[str, Path] = {}
    allowed = set(EXPECTED_FILE_TABLE.keys())

    for p in files:
        name = p.name
        if name not in allowed:
            raise ValueError(
                f"Unsupported file '{name}'. Allowed: {sorted(allowed)}"
            )
        table = EXPECTED_FILE_TABLE[name]
        table_to_path[table] = p

    return table_to_path


# Map UI rule headings (dummy rule ids) to concrete agent checks
# Value is the agent tool identifier we will call later.
RULE_TO_TOOL: Dict[str, str] = {
    # Segregation of Duties -> JE same user posted & approved
    "UAR-002": "je_same_user_post_approve",
    # Terminated User Access Testing -> terminated users with access
    "UAR-001": "terminated_users_with_access",
    # Unusual High-Value Transfers -> duplicate invoices (P2P)
    "TXN-101": "p2p_duplicate_invoices",
    # Generic audit bucket -> fictitious vendors
    "AUD-007": "fictitious_vendors",
}


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


# Agent-backed runtime (Step 2):
# - Validates files
# - Preloads CSVs
# - Iterates DUMMY_RULES and runs mapped checks
# - Emits UI-compatible events (rule_started, tool_call/result, rule_status, rule_completed, overall)
async def run_agent(files: List[Path]) -> None:
    try:
        table_paths = validate_and_map_files(files)

        # Preload datasets once using pandas (decoupled from agent wrappers)
        dfs: Dict[str, pd.DataFrame] = {}
        for table, path in table_paths.items():
            dfs[table] = pd.read_csv(path)

        def has_tables(req: List[str]) -> Optional[str]:
            missing = [t for t in req if t not in dfs]
            return ", ".join(missing) if missing else None

        async def _emit_thinking(rule_id: str, steps: List[str], delay: float = 0.1) -> None:
            for t in steps:
                await emit(Event("rule_status", rule_id=rule_id, data={"text": t}))
                await asyncio.sleep(delay)

        async def rule_lifecycle(
            rid: str,
            title: str,
            tag: str,
            fn: Optional[str],
        ) -> Optional[Finding]:
            start_ms = time.perf_counter()
            await emit(Event("rule_started", rule_id=rid, data={"title": title, "tag": tag}))
            await emit(Event("rule_status", rule_id=rid, data={"text": "Thinking..."}))
            # Simulated LLM streaming thoughts for UX; can be replaced with real streaming later
            await _emit_thinking(
                rid,
                [
                    "LLM: reading inputs",
                    "LLM: planning next step",
                    f"LLM: selecting tool {fn}" if fn else "LLM: no suitable tool found",
                ],
                delay=0.08,
            )

            findings = 0
            finding_model: Optional[Finding] = None
            try:
                if fn is None:
                    await emit(
                        Event(
                            "rule_status",
                            rule_id=rid,
                            data={"text": "No tool implemented for this rule yet"},
                        )
                    )
                elif fn == "je_same_user_post_approve":
                    missing = has_tables(["jes"])
                    if missing:
                        await emit(
                            Event(
                                "rule_status",
                                rule_id=rid,
                                data={"text": f"Missing datasets: {missing}"},
                            )
                        )
                    else:
                        await emit(Event("rule_status", rule_id=rid, data={"text": "LLM: invoking tool"}))
                        await emit(
                            Event(
                                "tool_call",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "args": {"table": "jes", "id_col": "je_id"},
                                },
                            )
                        )
                        df = dfs["jes"]
                        posted_by_col = "posted_by"
                        approved_by_col = "approved_by"
                        id_col = "je_id"
                        for col in [posted_by_col, approved_by_col, id_col]:
                            if col not in df.columns:
                                raise ValueError(f"Missing column '{col}' in jes")
                        finding_obj = compute_je_same_user_post_approve(
                            df, id_col=id_col, posted_by_col=posted_by_col, approved_by_col=approved_by_col
                        )
                        findings = int(finding_obj.count)
                        finding_model = finding_obj
                        await emit(
                            Event(
                                "tool_result",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "ok": True,
                                    "summary": f"{findings} matches",
                                    "ms": 0,
                                },
                            )
                        )

                elif fn == "p2p_duplicate_invoices":
                    missing = has_tables(["invoices"])
                    if missing:
                        await emit(
                            Event(
                                "rule_status",
                                rule_id=rid,
                                data={"text": f"Missing datasets: {missing}"},
                            )
                        )
                    else:
                        await emit(Event("rule_status", rule_id=rid, data={"text": "LLM: invoking tool"}))
                        await emit(
                            Event(
                                "tool_call",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "args": {
                                        "table": "invoices",
                                        "vendor_col": "vendor_id",
                                        "inv_col": "invoice_no",
                                        "amt_col": "amount",
                                    },
                                },
                            )
                        )
                        df = dfs["invoices"]
                        for col in ["vendor_id", "invoice_no", "amount"]:
                            if col not in df.columns:
                                raise ValueError(f"Missing column '{col}' in invoices")
                        finding_obj = compute_p2p_duplicate_invoices(
                            df, vendor_col="vendor_id", inv_col="invoice_no", amt_col="amount"
                        )
                        findings = int(finding_obj.count)
                        finding_model = finding_obj
                        await emit(
                            Event(
                                "tool_result",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "ok": True,
                                    "summary": f"{findings} duplicate groups",
                                    "ms": 0,
                                },
                            )
                        )

                elif fn == "fictitious_vendors":
                    missing = has_tables(["vendors", "employees"])
                    if missing:
                        await emit(
                            Event(
                                "rule_status",
                                rule_id=rid,
                                data={"text": f"Missing datasets: {missing}"},
                            )
                        )
                    else:
                        await emit(Event("rule_status", rule_id=rid, data={"text": "LLM: invoking tool"}))
                        await emit(
                            Event(
                                "tool_call",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "args": {
                                        "vendor_table": "vendors",
                                        "emp_table": "employees",
                                    },
                                },
                            )
                        )
                        v = dfs["vendors"].copy()
                        e = dfs["employees"].copy()
                        for col in ["address"]:
                            if col not in v.columns:
                                raise ValueError("Missing column 'address' in vendors")
                            if col not in e.columns:
                                raise ValueError("Missing column 'address' in employees")
                        if "vendor_id" not in v.columns:
                            raise ValueError("Missing column 'vendor_id' in vendors")
                        finding_obj = compute_fictitious_vendors(
                            v, e, v_addr="address", e_addr="address", v_id="vendor_id"
                        )
                        findings = int(finding_obj.count)
                        finding_model = finding_obj
                        await emit(
                            Event(
                                "tool_result",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "ok": True,
                                    "summary": f"{findings} address matches",
                                    "ms": 0,
                                },
                            )
                        )

                elif fn == "terminated_users_with_access":
                    missing = has_tables(["user_access", "employees"])
                    if missing:
                        await emit(
                            Event(
                                "rule_status",
                                rule_id=rid,
                                data={"text": f"Missing datasets: {missing}"},
                            )
                        )
                    else:
                        await emit(Event("rule_status", rule_id=rid, data={"text": "LLM: invoking tool"}))
                        await emit(
                            Event(
                                "tool_call",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "args": {
                                        "ua_table": "user_access",
                                        "users_table": "employees",
                                    },
                                },
                            )
                        )
                        ua = dfs["user_access"]
                        emp = dfs["employees"]
                        for col in ["user_id", "employment_status"]:
                            if col not in emp.columns:
                                raise ValueError(f"Missing column '{col}' in employees")
                        if "user_id" not in ua.columns or "active" not in ua.columns:
                            raise ValueError("Missing 'user_id' or 'active' in user_access")
                        finding_obj = compute_terminated_users_with_access(
                            ua, emp, user_id="user_id", status_col="employment_status", active_flag="active"
                        )
                        findings = int(finding_obj.count)
                        finding_model = finding_obj
                        await emit(
                            Event(
                                "tool_result",
                                rule_id=rid,
                                data={
                                    "name": fn,
                                    "ok": True,
                                    "summary": f"{findings} active accounts",
                                    "ms": 0,
                                },
                            )
                        )

                else:
                    await emit(
                        Event(
                            "rule_status",
                            rule_id=rid,
                            data={"text": f"Unknown tool mapping for {rid}"},
                        )
                    )

                # finalize rule
                dur_ms = int((time.perf_counter() - start_ms) * 1000)
                await emit(
                    Event(
                        "rule_completed",
                        rule_id=rid,
                        data={"findings": findings, "ms": dur_ms},
                    )
                )
                return finding_model
            except Exception as e:
                await emit(Event("rule_failed", rule_id=rid, data={"error": str(e)}))
                return None

        total_rules = len(DUMMY_RULES)
        completed = 0
        total_findings = 0
        await emit(
            Event(
                "overall",
                data={"completed": completed, "total": total_rules, "findings": total_findings},
            )
        )

        # Iterate rules, using RULE_TO_TOOL where available
        collected: List[Finding] = []
        for rule in DUMMY_RULES:
            rid = str(rule.get("id", ""))
            title = str(rule.get("title", ""))
            tag = str(rule.get("tag", ""))
            tool = RULE_TO_TOOL.get(rid)
            fobj = await rule_lifecycle(rid, title, tag, tool)
            if fobj is not None:
                collected.append(fobj)
                total_findings += int(fobj.count)
            completed += 1
            await emit(
                Event(
                    "overall",
                    data={
                        "completed": completed,
                        "total": total_rules,
                        "findings": total_findings,
                    },
                )
            )

            await asyncio.sleep(0.05)

        # Build an Audit-like report payload for UI
        audit_findings = [f.model_dump() for f in collected]
        total_flags = sum(int(f["count"]) for f in audit_findings)
        audit = {
            "findings": audit_findings,
            "summary": f"{len(audit_findings)} tests run, {total_flags} total flags.",
        }
        def sev_sum(level: str) -> int:
            return sum(int(f.get("count", 0)) for f in audit_findings if str(f.get("severity", "")).lower() == level)
        report = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "summary": audit["summary"],
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
            "raw": audit,
        }

        await emit(Event("done", data={"report": report}))
    except asyncio.CancelledError:
        # Swallow cancellation cleanly when user navigates away
        return
    except Exception as e:
        # Emit a terminal failure and finish to avoid UI hanging
        await emit(Event("rule_failed", rule_id=None, data={"error": str(e)}))
        await emit(Event("done"))
