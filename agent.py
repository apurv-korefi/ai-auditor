# audit_agent.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel

from agents import (
    Agent,
    ItemHelpers,
    MessageOutputItem,
    Runner,
    RunContextWrapper,
    RunHooks,
    function_tool,
)

from compute_helpers import (
    je_same_user_post_approve as compute_je_same_user_post_approve,
    p2p_duplicate_invoices as compute_p2p_duplicate_invoices,
    fictitious_vendors as compute_fictitious_vendors,
    terminated_users_with_access as compute_terminated_users_with_access,
)

# ---------------- Context ----------------
Emitter = Callable[[str, Dict[str, Any]], None]


@dataclass
class AuditContext:
    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)
    emit: Optional[Emitter] = None  # optional: forward lifecycle events to UI/CLI


def _emit(
    ctx: RunContextWrapper["AuditContext"], event: str, payload: Dict[str, Any]
) -> None:
    try:
        if ctx.context.emit:
            ctx.context.emit(event, payload)
    except Exception:
        pass  # logging must never break the run


# ---------------- Output models ----------------
class Finding(BaseModel):
    test: str
    severity: str
    count: int
    sample_ids: List[str]
    notes: Optional[str] = None


class AuditReport(BaseModel):
    findings: List[Finding]
    summary: str


# ---------------- Tools ----------------
@function_tool
def load_csv(ctx: RunContextWrapper[AuditContext], name: str, path: str) -> str:
    """Load a CSV into memory for later tests."""
    df = pd.read_csv(path)
    ctx.context.tables[name] = df
    _emit(ctx, "tool_note", {"tool": "load_csv", "name": name, "rows": len(df)})
    return f"Loaded {name} with {len(df)} rows."


@function_tool
def je_same_user_post_approve(
    ctx: RunContextWrapper[AuditContext],
    table: str = "jes",
    id_col: str = "je_id",
    posted_by_col: str = "posted_by",
    approved_by_col: str = "approved_by",
) -> str:
    """Flag JEs where poster == approver."""
    df = _require(ctx, table)
    count, sample = compute_je_same_user_post_approve(
        df, id_col, posted_by_col, approved_by_col
    )
    finding = Finding(
        test="JE same user posted & approved",
        severity="high",
        count=count,
        sample_ids=sample,
    )
    return finding.model_dump_json()


@function_tool
def p2p_duplicate_invoices(
    ctx: RunContextWrapper[AuditContext],
    table: str = "invoices",
    vendor_col: str = "vendor_id",
    inv_col: str = "invoice_no",
    amt_col: str = "amount",
) -> str:
    """Duplicate invoices: same vendor + invoice_no + amount."""
    df = _require(ctx, table)
    count, ids = compute_p2p_duplicate_invoices(df, vendor_col, inv_col, amt_col)
    finding = Finding(
        test="P2P duplicate invoices",
        severity="high",
        count=count,
        sample_ids=list(ids),
    )
    return finding.model_dump_json()


@function_tool
def fictitious_vendors(
    ctx: RunContextWrapper[AuditContext],
    vendor_table: str = "vendors",
    emp_table: str = "employees",
    v_addr: str = "address",
    e_addr: str = "address",
    v_id: str = "vendor_id",
) -> str:
    """Vendor address matches an employee address."""
    v = _require(ctx, vendor_table)
    e = _require(ctx, emp_table)
    count, sample = compute_fictitious_vendors(v, e, v_addr, e_addr, v_id)
    finding = Finding(
        test="Fictitious vendor (address match)",
        severity="medium",
        count=count,
        sample_ids=sample,
    )
    return finding.model_dump_json()


@function_tool
def terminated_users_with_access(
    ctx: RunContextWrapper[AuditContext],
    ua_table: str = "user_access",
    users_table: str = "employees",
    user_id: str = "user_id",
    status_col: str = "employment_status",
    active_flag: str = "active",
) -> str:
    """Terminated employees who still have active access."""
    ua = _require(ctx, ua_table)
    emp = _require(ctx, users_table)
    count, sample = compute_terminated_users_with_access(
        ua, emp, user_id, status_col, active_flag
    )
    finding = Finding(
        test="Terminated users with access",
        severity="critical",
        count=count,
        sample_ids=sample,
    )
    return finding.model_dump_json()


@function_tool
def compile_report(findings_json: List[str]) -> str:
    """Combine tool outputs (JSON strings) into a single AuditReport JSON."""
    parsed = [Finding.model_validate_json(f) for f in findings_json]
    total_flags = sum(f.count for f in parsed)
    report = AuditReport(
        findings=parsed,
        summary=f"{len(parsed)} tests run, {total_flags} total flags.",
    )
    return report.model_dump_json()


# ---------------- Agent ----------------
auditor = Agent[AuditContext](
    name="AI Auditor",
    model="gpt-5",
    instructions=(
        "You are an internal audit agent. "
        "Use the available tools to load CSVs and run tests. "
        "When tests return JSON Finding objects, pass them into compile_report "
        "to produce a single JSON AuditReport. Do not invent columns; "
        "ask for the right file/column names if missing."
    ),
    tools=[
        load_csv,
        je_same_user_post_approve,
        p2p_duplicate_invoices,
        fictitious_vendors,
        terminated_users_with_access,
        compile_report,
    ],
)


# ---------------- Hooks: forward lifecycle to ctx.emit ----------------
class EmitHooks(RunHooks[AuditContext]):
    async def on_agent_start(
        self, context: RunContextWrapper[AuditContext], agent: Agent
    ) -> None:
        _emit(context, "agent_start", {"agent": agent.name})

    async def on_llm_start(
        self,
        context: RunContextWrapper[AuditContext],
        agent: Agent,
        system_prompt: Optional[str],
        input_items: list[Any],
    ) -> None:
        _emit(context, "llm_start", {"agent": agent.name, "inputs": len(input_items)})

    async def on_tool_start(
        self, context: RunContextWrapper[AuditContext], agent: Agent, tool
    ) -> None:
        _emit(
            context,
            "tool_start",
            {"agent": agent.name, "tool": getattr(tool, "name", str(tool))},
        )

    async def on_tool_end(
        self, context: RunContextWrapper[AuditContext], agent: Agent, tool, result: str
    ) -> None:
        _emit(
            context,
            "tool_end",
            {
                "agent": agent.name,
                "tool": getattr(tool, "name", str(tool)),
                "result_preview": str(result)[:160],
            },
        )

    async def on_handoff(
        self,
        context: RunContextWrapper[AuditContext],
        from_agent: Agent,
        to_agent: Agent,
    ) -> None:
        _emit(context, "handoff", {"from": from_agent.name, "to": to_agent.name})

    async def on_agent_end(
        self, context: RunContextWrapper[AuditContext], agent: Agent, output: Any
    ) -> None:
        # Include usage (SDK exposes this on the context in hooks)
        u = context.usage
        _emit(
            context,
            "agent_end",
            {
                "agent": agent.name,
                "output_preview": str(output)[:160],
                "usage": {
                    "requests": u.requests,
                    "input_tokens": u.input_tokens,
                    "output_tokens": u.output_tokens,
                    "total_tokens": u.total_tokens,
                },
            },
        )


# ---------------- Utilities ----------------
def _require(ctx: RunContextWrapper[AuditContext], name: str) -> pd.DataFrame:
    if name not in ctx.context.tables:
        raise ValueError(f"Table '{name}' not loaded. Call load_csv first.")
    return ctx.context.tables[name]


# Public helpers for callers embedding this module
def run_audit_stream(
    plan: str,
    *,
    emit: Optional[Emitter] = None,
    hooks: Optional[RunHooks[AuditContext]] = None,
):
    """
    Run the agent in streaming mode and return RunResultStreaming.
    Callers can iterate result.stream_events() to drive their own UI.
    """
    ctx = AuditContext(emit=emit)
    return Runner.run_streamed(
        auditor, input=plan, context=ctx, hooks=hooks or EmitHooks()
    )


def run_audit(
    plan: str,
    *,
    emit: Optional[Emitter] = None,
    hooks: Optional[RunHooks[AuditContext]] = None,
):
    """
    Non-streaming convenience wrapper (returns RunResult).
    """
    ctx = AuditContext(emit=emit)
    return Runner.run(auditor, input=plan, context=ctx, hooks=hooks or EmitHooks())


# ---------------- CLI demo (streamed) ----------------
if __name__ == "__main__":
    import asyncio

    async def main():
        def console_emit(evt: str, payload: Dict[str, Any]) -> None:
            print(f"[{evt}] {payload}")

        ctx_plan = (
            "1) load_csv(name='jes', path='data/journal_entries.csv')\n"
            "2) load_csv(name='invoices', path='data/invoices.csv')\n"
            "3) load_csv(name='vendors', path='data/vendors.csv')\n"
            "4) load_csv(name='employees', path='data/employees.csv')\n"
            "5) load_csv(name='user_access', path='data/user_access.csv')\n"
            "Then run je_same_user_post_approve, p2p_duplicate_invoices, "
            "fictitious_vendors, terminated_users_with_access, and compile_report."
        )

        result = run_audit_stream(ctx_plan, emit=console_emit)

        print("=== Streaming ===")
        async for ev in result.stream_events():
            if ev.type == "run_item_stream_event":
                item = ev.item
                if isinstance(item, MessageOutputItem):
                    print(
                        f"-- Assistant message:\n{ItemHelpers.text_message_output(item)}"
                    )
                else:
                    # tool calls, outputs, handoffs, reasoning, etc.
                    print(f"-- {ev.name}: {type(item).__name__}")
            elif ev.type == "agent_updated_stream_event":
                print(f"-- Agent switched to: {ev.new_agent.name}")
            # raw_response_event is available if you want raw token deltas

        print("=== Done ===")
        print("\n=== AUDIT REPORT (JSON) ===")
        print(result.final_output)

    asyncio.run(main())
