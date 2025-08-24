from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import pandas as pd
from openai.types.shared import Reasoning as ReasoningConfig  # SDK "Reasoning" config
from agents import (
    Agent,
    MessageOutputItem,
    ModelSettings,
    ReasoningItem,
    RunContextWrapper,
    RunHooks,
    Runner,
    ToolCallItem,
    ToolCallOutputItem,
    function_tool,
)

# ---- Pure compute + schemas live in compute_helpers.py (import and use) ----
from compute_helpers import (  # you already created this file earlier
    AuditReport,
    Finding,
    compute_fictitious_vendors,
    compute_je_same_user_post_approve,
    compute_p2p_duplicate_invoices,
    compute_terminated_users_with_access,
)


# ---------- Minimal context (shared state for tools) ----------
@dataclass
class AuditContext:
    tables: Dict[str, pd.DataFrame] = field(default_factory=dict)


# ---------- Tools (thin wrappers; no custom emitters) ----------
@function_tool
def load_csv(ctx: RunContextWrapper[AuditContext], name: str, path: str) -> str:
    """Load a CSV into context.tables for later tests."""
    df = pd.read_csv(path)
    ctx.context.tables[name] = df
    return f"Loaded {name} with {len(df)} rows."


def _require(ctx: RunContextWrapper[AuditContext], name: str) -> pd.DataFrame:
    if name not in ctx.context.tables:
        raise ValueError(f"Table '{name}' not loaded. Call load_csv first.")
    return ctx.context.tables[name]


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
    finding = compute_je_same_user_post_approve(
        df, id_col=id_col, posted_by_col=posted_by_col, approved_by_col=approved_by_col
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
    """Detect duplicate invoices: same (vendor, invoice_no, amount)."""
    df = _require(ctx, table)
    finding = compute_p2p_duplicate_invoices(
        df, vendor_col=vendor_col, inv_col=inv_col, amt_col=amt_col
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
    finding = compute_fictitious_vendors(v, e, v_addr=v_addr, e_addr=e_addr, v_id=v_id)
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
    finding = compute_terminated_users_with_access(
        ua, emp, user_id=user_id, status_col=status_col, active_flag=active_flag
    )
    return finding.model_dump_json()


@function_tool
def compile_report(findings_json: List[str]) -> str:
    """Combine tool outputs (JSON strings) into a single AuditReport JSON."""
    parsed = [Finding.model_validate_json(f) for f in findings_json]
    total_flags = sum(f.count for f in parsed)
    report = AuditReport(
        findings=parsed, summary=f"{len(parsed)} tests run, {total_flags} total flags."
    )
    return report.model_dump_json()


# ---------- Agent (enable reasoning summaries + encrypted chain if present) ----------
AUDITOR = Agent[AuditContext](
    name="AI Auditor",
    model="gpt-5",
    instructions=(
        "You are an internal audit agent. Use the available tools to load CSVs and run tests. "
        "When tests return JSON Finding objects, pass them into compile_report to produce a single JSON AuditReport. "
        "Do not invent columns; if a column/table is missing, raise a clear error."
    ),
    tools=[
        load_csv,
        je_same_user_post_approve,
        p2p_duplicate_invoices,
        fictitious_vendors,
        terminated_users_with_access,
        compile_report,
    ],
    model_settings=ModelSettings(
        # Ask the model to include a reasoning summary (shown in stream as ReasoningItem)
        reasoning=ReasoningConfig(effort="low", summary="auto"),
        # If available, include encrypted reasoning content (safe to ignore if not used)
        response_include=["reasoning.encrypted_content"],
        verbosity="low",
        truncation="auto",
    ),
)


# ---------- Hooks: lifecycle pings (agent start/end, tool start/end) ----------
class AuditRunHooks(RunHooks[AuditContext]):
    async def on_agent_start(
        self, context: RunContextWrapper[AuditContext], agent: Agent[AuditContext]
    ) -> None:
        print(f"[agent_start] {{'agent': '{agent.name}'}}")

    async def on_agent_end(
        self,
        context: RunContextWrapper[AuditContext],
        agent: Agent[AuditContext],
        output: Any,
    ) -> None:
        preview = (output if isinstance(output, str) else str(output))[:80].replace(
            "\n", " "
        )
        print(f"[agent_end] {{'agent': '{agent.name}', 'output_preview': '{preview}'}}")

    async def on_tool_start(
        self, context: RunContextWrapper[AuditContext], agent: Agent[AuditContext], tool
    ) -> None:
        print(f"[tool_start] {{'agent': '{agent.name}', 'tool': '{tool.name}'}}")

    async def on_tool_end(
        self,
        context: RunContextWrapper[AuditContext],
        agent: Agent[AuditContext],
        tool,
        result: str,
    ) -> None:
        preview = (result if isinstance(result, str) else str(result))[:80].replace(
            "\n", " "
        )
        print(
            f"[tool_end] {{'agent': '{agent.name}', 'tool': '{tool.name}', 'result_preview': '{preview}'}}"
        )


# ---------- Streaming helpers ----------
def _extract_reasoning_summary_text(item: ReasoningItem) -> Optional[str]:
    """
    Try to pull a human-readable reasoning summary from the ReasoningItem.
    Works whether the provider returns structured summary parts or not.
    """
    raw = item.raw_item
    # Newer Responses API returns `summary: list[...]` parts with `.text`
    summary_parts = getattr(raw, "summary", None)
    if summary_parts:
        texts = [
            getattr(p, "text", None) for p in summary_parts if getattr(p, "text", None)
        ]
        if texts:
            return " ".join(texts)
    # Fallback: stringify raw (short)
    return None


""""""


async def stream_run(
    plan_or_input: str,
    *,
    context: Optional[AuditContext] = None,
    hooks: Optional[RunHooks[AuditContext]] = None,
) -> AsyncIterator[Dict[str, Any]]:
    """
    Start the agent in streaming mode and yield normalized event dicts that any UI can consume.
    This is the same stream the CLI uses below.
    """
    ctx = context or AuditContext()
    # Runner.run_streamed returns a streaming result object (not awaitable)
    result = Runner.run_streamed(
        starting_agent=AUDITOR,
        input=plan_or_input,
        context=ctx,
        hooks=hooks,
    )

    async for ev in result.stream_events():
        # Agent switch (handoffs etc.)
        if ev.type == "agent_updated_stream_event":
            yield {"type": "agent_switched", "agent": ev.new_agent.name}
            continue

        if ev.type == "run_item_stream_event":
            item = ev.item

            if isinstance(item, ReasoningItem):
                text = _extract_reasoning_summary_text(item)
                yield {"type": "reasoning", "text": text or ""}
                continue

            if isinstance(item, ToolCallItem):
                tool_name = type(item).__name__
                yield {
                    "type": "tool_called",
                    "tool": tool_name,
                }
                continue

            if isinstance(item, ToolCallOutputItem):
                tool_name = type(item).__name__
                yield {
                    "type": "tool_output",
                    "tool": tool_name,
                    "output_preview": (str(item)[:80]).replace("\n", " "),
                }
                continue

            if isinstance(item, MessageOutputItem):
                yield {
                    "type": "assistant_message",
                    "text_preview": (str(item)[:80]).replace("\n", " "),
                }
                continue

        # Raw deltas (ResponseTextDeltaEvent, ReasoningSummaryTextDeltaEvent) can be handled if you want token streams.
        if ev.type == "raw_response_event":
            # If you want token-by-token text, check event.data types here.
            pass

    # Final output payload for convenience
    usage = getattr(result, "usage", None)
    yield {"type": "done", "final_output": result.final_output, "usage": usage}


# ---------- CLI entrypoint ----------
async def _main() -> None:
    ctx = AuditContext()
    plan = (
        "1) load_csv(name='jes', path='data/journal_entries.csv')\n"
        "2) load_csv(name='invoices', path='data/invoices.csv')\n"
        "3) load_csv(name='vendors', path='data/vendors.csv')\n"
        "4) load_csv(name='employees', path='data/employees.csv')\n"
        "5) load_csv(name='user_access', path='data/user_access.csv')\n"
        "Then run je_same_user_post_approve, p2p_duplicate_invoices, "
        "fictitious_vendors, terminated_users_with_access, and compile_report."
    )

    print("=== Streaming ===")
    async for evt in stream_run(plan, context=ctx, hooks=AuditRunHooks()):
        t = evt["type"]
        if t == "agent_switched":
            print(f"-- Agent switched to: {evt['agent']}")
        elif t == "reasoning":
            if evt["text"]:
                print(f"-- reasoning: {evt['text']}")
            else:
                print(f"-- reasoning: (no summary provided)")
        elif t == "done":
            print("\n=== AUDIT REPORT (JSON) ===")
            print(evt["final_output"])
            # Optional: print usage
            if evt.get("usage"):
                print(f"\n[usage] {evt['usage']}")


if __name__ == "__main__":
    asyncio.run(_main())
