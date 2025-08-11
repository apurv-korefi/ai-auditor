from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd
from pydantic import BaseModel
from agents import Agent, Runner, function_tool, RunContextWrapper

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
@function_tool
def load_csv(ctx: RunContextWrapper[AuditContext], name: str, path: str) -> str:
    """Load a CSV into memory for later tests.
    Args:
        name: Short handle, e.g. 'jes' or 'vendors'
        path: File path to CSV
    """
    ctx.context.tables[name] = pd.read_csv(path)
    return f"Loaded {name} with {len(ctx.context.tables[name])} rows."

@function_tool
def je_same_user_post_approve(ctx: RunContextWrapper[AuditContext],
                              table: str = "jes",
                              id_col: str = "je_id",
                              posted_by_col: str = "posted_by",
                              approved_by_col: str = "approved_by") -> str:
    """Flag JEs where poster == approver."""
    df = _require(ctx, table)
    mask = df[posted_by_col].astype(str).str.lower() == df[approved_by_col].astype(str).str.lower()
    hits = df[mask]
    sample = hits[id_col].astype(str).head(10).tolist()
    return Finding(test="JE same user posted & approved",
                   severity="high", count=len(hits),
                   sample_ids=sample).model_dump_json()

@function_tool
def p2p_duplicate_invoices(ctx: RunContextWrapper[AuditContext],
                           table: str = "invoices",
                           vendor_col: str = "vendor_id",
                           inv_col: str = "invoice_no",
                           amt_col: str = "amount") -> str:
    """Detect duplicate invoices: same vendor + invoice_no + amount."""
    df = _require(ctx, table)
    grp = df.groupby([vendor_col, inv_col, amt_col], dropna=False).size().reset_index(name="n")
    dups = grp[grp["n"] > 1]
    ids = (df.merge(dups[[vendor_col, inv_col, amt_col]],
           on=[vendor_col, inv_col, amt_col]))[inv_col].astype(str).unique().tolist()[:10]
    return Finding(test="P2P duplicate invoices", severity="high",
                   count=len(dups), sample_ids=list(ids)).model_dump_json()

@function_tool
def fictitious_vendors(ctx: RunContextWrapper[AuditContext],
                       vendor_table: str = "vendors",
                       emp_table: str = "employees",
                       v_addr: str = "address",
                       e_addr: str = "address",
                       v_id: str = "vendor_id") -> str:
    """Vendor address matches an employee address."""
    v = _require(ctx, vendor_table).copy()
    e = _require(ctx, emp_table).copy()
    v[v_addr] = v[v_addr].astype(str).str.strip().str.lower()
    e[e_addr] = e[e_addr].astype(str).str.strip().str.lower()
    matches = v.merge(e, left_on=v_addr, right_on=e_addr, how="inner")
    sample = matches[v_id].astype(str).head(10).tolist()
    return Finding(test="Fictitious vendor (address match)", severity="medium",
                   count=len(matches), sample_ids=sample).model_dump_json()

@function_tool
def terminated_users_with_access(ctx: RunContextWrapper[AuditContext],
                                 ua_table: str = "user_access",
                                 users_table: str = "employees",
                                 user_id: str = "user_id",
                                 status_col: str = "employment_status",
                                 active_flag: str = "active") -> str:
    """Terminated employees who still have active access."""
    ua = _require(ctx, ua_table)
    emp = _require(ctx, users_table)
    term = emp[emp[status_col].str.lower().eq("terminated")]
    merged = ua.merge(term[[user_id]], on=user_id, how="inner")
    still_active = merged[merged[active_flag] == True]
    sample = still_active[user_id].astype(str).head(10).tolist()
    return Finding(test="Terminated users with access", severity="critical",
                   count=len(still_active), sample_ids=sample).model_dump_json()

@function_tool
def compile_report(findings_json: List[str]) -> str:
    """Combine tool outputs (JSON strings) into a single AuditReport JSON."""
    parsed = [Finding.model_validate_json(f) for f in findings_json]
    total_flags = sum(f.count for f in parsed)
    report = AuditReport(
        findings=parsed,
        summary=f"{len(parsed)} tests run, {total_flags} total flags."
    )
    return report.model_dump_json()

# ---------- Agent ----------
auditor = Agent[AuditContext](
    name="AI Auditor",
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

