from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
import pandas as pd
from pydantic import BaseModel
from agents import Agent, Runner, function_tool, RunContextWrapper

# ---------- Optional tool event emitter (set by runtime for UI streaming) ----------
ToolEvent = Dict[str, Any]
ToolEventEmitter = Callable[[str, ToolEvent], None]
_TOOL_EVENT_EMITTER: Optional[ToolEventEmitter] = None


def set_tool_event_emitter(emitter: Optional[ToolEventEmitter]) -> None:
    global _TOOL_EVENT_EMITTER
    _TOOL_EVENT_EMITTER = emitter


def _emit_tool(event_type: str, payload: ToolEvent) -> None:
    try:
        if _TOOL_EVENT_EMITTER:
            _TOOL_EVENT_EMITTER(event_type, payload)
    except Exception:
        # never let UI wiring break tool execution
        pass


# ---------- Context to share state across tools ----------
@dataclass
class AuditContext:
    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)


# ---------- Simple output schema so the agent returns strict JSON ----------
class Finding(BaseModel):
    test: str
    severity: str
    count: int
    sample_ids: List[str]
    notes: Optional[str] = None


class AuditReport(BaseModel):
    findings: List[Finding]
    summary: str


# ---------- Utility ----------
def _require(ctx: RunContextWrapper[AuditContext], name: str) -> pd.DataFrame:
    if name not in ctx.context.tables:
        raise ValueError(f"Table '{name}' not loaded. Call load_csv first.")
    return ctx.context.tables[name]


# ---------- Tools ----------
def compute_je_same_user_post_approve(
    df: pd.DataFrame,
    id_col: str = "je_id",
    posted_by_col: str = "posted_by",
    approved_by_col: str = "approved_by",
) -> Finding:
    mask = (
        df[posted_by_col].astype(str).str.lower()
        == df[approved_by_col].astype(str).str.lower()
    )
    hits = df[mask]
    sample = hits[id_col].astype(str).head(10).tolist()
    return Finding(
        test="JE same user posted & approved",
        severity="high",
        count=len(hits),
        sample_ids=sample,
    )


def compute_p2p_duplicate_invoices(
    df: pd.DataFrame,
    vendor_col: str = "vendor_id",
    inv_col: str = "invoice_no",
    amt_col: str = "amount",
) -> Finding:
    grp = (
        df.groupby([vendor_col, inv_col, amt_col], dropna=False)
        .size()
        .reset_index(name="n")
    )
    dups = grp[grp["n"] > 1]
    ids = (
        (
            df.merge(
                dups[[vendor_col, inv_col, amt_col]], on=[vendor_col, inv_col, amt_col]
            )
        )[inv_col]
        .astype(str)
        .unique()
        .tolist()[:10]
    )
    return Finding(
        test="P2P duplicate invoices",
        severity="high",
        count=len(dups),
        sample_ids=list(ids),
    )


def compute_fictitious_vendors(
    v: pd.DataFrame,
    e: pd.DataFrame,
    v_addr: str = "address",
    e_addr: str = "address",
    v_id: str = "vendor_id",
) -> Finding:
    v = v.copy()
    e = e.copy()
    v[v_addr] = v[v_addr].astype(str).str.strip().str.lower()
    e[e_addr] = e[e_addr].astype(str).str.strip().str.lower()
    matches = v.merge(e, left_on=v_addr, right_on=e_addr, how="inner")
    sample = matches[v_id].astype(str).head(10).tolist()
    return Finding(
        test="Fictitious vendor (address match)",
        severity="medium",
        count=len(matches),
        sample_ids=sample,
    )


def compute_terminated_users_with_access(
    ua: pd.DataFrame,
    emp: pd.DataFrame,
    user_id: str = "user_id",
    status_col: str = "employment_status",
    active_flag: str = "active",
) -> Finding:
    term = emp[emp[status_col].astype(str).str.lower().eq("terminated")]
    merged = ua.merge(term[[user_id]], on=user_id, how="inner")
    still_active = merged[merged[active_flag] == True]
    sample = still_active[user_id].astype(str).head(10).tolist()
    return Finding(
        test="Terminated users with access",
        severity="critical",
        count=len(still_active),
        sample_ids=sample,
    )


@function_tool
def load_csv(ctx: RunContextWrapper[AuditContext], name: str, path: str) -> str:
    """Load a CSV into memory for later tests.
    Args:
        name: Short handle, e.g. 'jes' or 'vendors'
        path: File path to CSV
    """
    _emit_tool("tool_call", {"name": "load_csv", "args": {"name": name, "path": path}})
    ctx.context.tables[name] = pd.read_csv(path)
    summary = f"Loaded {name} with {len(ctx.context.tables[name])} rows."
    _emit_tool(
        "tool_result",
        {"name": "load_csv", "ok": True, "summary": summary, "ms": 0},
    )
    return summary


@function_tool
def je_same_user_post_approve(
    ctx: RunContextWrapper[AuditContext],
    table: str = "jes",
    id_col: str = "je_id",
    posted_by_col: str = "posted_by",
    approved_by_col: str = "approved_by",
) -> str:
    """Flag JEs where poster == approver."""
    _emit_tool(
        "tool_call",
        {
            "name": "je_same_user_post_approve",
            "args": {
                "table": table,
                "id_col": id_col,
                "posted_by_col": posted_by_col,
                "approved_by_col": approved_by_col,
            },
        },
    )
    df = _require(ctx, table)
    finding = compute_je_same_user_post_approve(
        df, id_col=id_col, posted_by_col=posted_by_col, approved_by_col=approved_by_col
    )
    _emit_tool(
        "tool_result",
        {
            "name": "je_same_user_post_approve",
            "ok": True,
            "summary": f"{finding.count} matches",
            "finding": finding.model_dump(),
            "ms": 0,
        },
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
    """Detect duplicate invoices: same vendor + invoice_no + amount."""
    _emit_tool(
        "tool_call",
        {
            "name": "p2p_duplicate_invoices",
            "args": {
                "table": table,
                "vendor_col": vendor_col,
                "inv_col": inv_col,
                "amt_col": amt_col,
            },
        },
    )
    df = _require(ctx, table)
    finding = compute_p2p_duplicate_invoices(
        df, vendor_col=vendor_col, inv_col=inv_col, amt_col=amt_col
    )
    _emit_tool(
        "tool_result",
        {
            "name": "p2p_duplicate_invoices",
            "ok": True,
            "summary": f"{finding.count} duplicate groups",
            "finding": finding.model_dump(),
            "ms": 0,
        },
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
    _emit_tool(
        "tool_call",
        {
            "name": "fictitious_vendors",
            "args": {
                "vendor_table": vendor_table,
                "emp_table": emp_table,
                "v_addr": v_addr,
                "e_addr": e_addr,
                "v_id": v_id,
            },
        },
    )
    v = _require(ctx, vendor_table)
    e = _require(ctx, emp_table)
    finding = compute_fictitious_vendors(v, e, v_addr=v_addr, e_addr=e_addr, v_id=v_id)
    _emit_tool(
        "tool_result",
        {
            "name": "fictitious_vendors",
            "ok": True,
            "summary": f"{finding.count} address matches",
            "finding": finding.model_dump(),
            "ms": 0,
        },
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
    _emit_tool(
        "tool_call",
        {
            "name": "terminated_users_with_access",
            "args": {
                "ua_table": ua_table,
                "users_table": users_table,
                "user_id": user_id,
                "status_col": status_col,
                "active_flag": active_flag,
            },
        },
    )
    ua = _require(ctx, ua_table)
    emp = _require(ctx, users_table)
    finding = compute_terminated_users_with_access(
        ua, emp, user_id=user_id, status_col=status_col, active_flag=active_flag
    )
    _emit_tool(
        "tool_result",
        {
            "name": "terminated_users_with_access",
            "ok": True,
            "summary": f"{finding.count} active accounts",
            "finding": finding.model_dump(),
            "ms": 0,
        },
    )
    return finding.model_dump_json()


@function_tool
def compile_report(findings_json: List[str]) -> str:
    """Combine tool outputs (JSON strings) into a single AuditReport JSON."""
    _emit_tool("tool_call", {"name": "compile_report", "args": {"n": len(findings_json)}})
    parsed = [Finding.model_validate_json(f) for f in findings_json]
    total_flags = sum(f.count for f in parsed)
    report = AuditReport(
        findings=parsed, summary=f"{len(parsed)} tests run, {total_flags} total flags."
    )
    _emit_tool(
        "tool_result",
        {
            "name": "compile_report",
            "ok": True,
            "summary": report.summary,
            "ms": 0,
        },
    )
    return report.model_dump_json()


# ---------- Agent ----------
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
    # Tip: you can also enforce structured model output via AgentOutputSchema,
    # see docs. For MVP we'll just emit JSON from tools.
)

# ---------- Demo runner ----------
if __name__ == "__main__":
    import asyncio

    async def main():
        ctx = AuditContext()
        # Replace paths with your hackathon CSVs
        plan = (
            "1) load_csv(name='jes', path='data/journal_entries.csv')\n"
            "2) load_csv(name='invoices', path='data/invoices.csv')\n"
            "3) load_csv(name='vendors', path='data/vendors.csv')\n"
            "4) load_csv(name='employees', path='data/employees.csv')\n"
            "5) load_csv(name='user_access', path='data/user_access.csv')\n"
            "Then run je_same_user_post_approve, p2p_duplicate_invoices, "
            "fictitious_vendors, terminated_users_with_access, and "
            "compile_report on their outputs."
        )
        result = await Runner.run(auditor, input=plan, context=ctx)
        print("\n=== AUDIT REPORT (JSON) ===")
        print(result.final_output)

    asyncio.run(main())
