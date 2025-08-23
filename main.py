# main.py
# Fast, fully local 3‑page frontend in Python using NiceGUI
# Pages:
# 1) /upload        — upload a file
# 2) /processing    — shows a streaming agent-like response
# 3) /report        — shows a report and action items
#
# How to run:
#   pip install nicegui
#   python main.py
# Then open http://127.0.0.1:8080/upload

import asyncio
import json
import os
import secrets
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Dict

from nicegui import app, ui
from nicegui.events import UploadEventArguments

from dataclasses import dataclass
from typing import Literal, Optional, Any, Dict, List
import random
import time

APP_TITLE = "AI Audit Assitant"
UPLOAD_DIR = Path("./uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Random secret is fine for local runs; you can also set NICEGUI_STORAGE_SECRET in env
STORAGE_SECRET = os.getenv("NICEGUI_STORAGE_SECRET", secrets.token_urlsafe(32))


def apply_theme() -> None:
    # Quasar brand colors (buttons default to primary)
    ui.colors(primary="#0f172a")  # Tailwind slate-900


def user_store() -> dict:
    """Per-session storage on server side; requires storage_secret in ui.run."""
    return app.storage.user


async def fake_agent_stream(file_path: Path) -> AsyncGenerator[str, None]:
    """Simulate a token stream without using newline escapes."""
    lines = [
        f"Processing file: {file_path.name}",
        "Analyzing content...",
        "Running model passes...",
        "Extracting insights...",
        "Finalizing...",
        "",
        (
            "Agent: The document was parsed successfully. Key sections identified, "
            "entities extracted, and a draft report is ready."
        ),
    ]
    for line in lines:
        chunk = (line + " EOL ") if line else "EOL "
        for ch in chunk:
            yield ch
            await asyncio.sleep(0.01)


def build_report(file_path: Path) -> Dict:
    """Create a demo report structure."""
    now = datetime.now().isoformat(timespec="seconds")
    return {
        "file": file_path.name,
        "generated_at": now,
        "summary": (
            "This is a placeholder summary. Replace build_report to use your actual outputs."
        ),
        "metrics": {"pages": 3, "entities": 12, "confidence": 0.97},
        "action_items": [
            {"title": "Verify extracted totals", "owner": "You", "due": "Tomorrow"},
            {"title": "Export CSV & share", "owner": "You", "due": "Today"},
            {"title": "Open questions for client", "owner": "Team", "due": "Next week"},
        ],
    }


def header() -> None:
    apply_theme()
    with ui.header().classes("bg-primary"):  # use brand color
        with ui.row().classes("w-full justify-center"):  # centers the inner container
            with ui.row().classes("w-full max-w-5xl items-center justify-between px-4"):
                ui.label(APP_TITLE).classes("text-xl font-semibold text-white")
                # put nav/actions here later if you want; if empty, the title still sits in the centered container


@ui.page("/upload")
def upload_page() -> None:
    header()
    with ui.column().classes("w-full max-w-5xl mx-auto p-6 bg-slate-50 gap-3"):
        ui.label("Upload").classes("text-2xl font-bold")
        ui.label("Choose one or more files to process.").classes("text-gray-600")

        store = user_store()
        selected = ui.label().classes("text-sm text-gray-500")

        files = list(store.get("file_paths") or [])
        if files:
            selected.text = f"Selected: {len(files)} file(s)"

        def on_upload(e: UploadEventArguments) -> None:
            dest = UPLOAD_DIR / e.name
            dest.write_bytes(e.content.read())
            files = list(store.get("file_paths") or [])
            files.append(str(dest))
            store["file_paths"] = files
            selected.text = f"Selected: {len(files)} file(s)"
            go_btn.enable()
            ui.notify(f"Added {dest.name}", type="positive")

        ui.upload(on_upload=on_upload, auto_upload=True).props(
            "accept=*/*, multiple, max-files=20"
        ).classes("w-full").style("max-width: none")

        def clear() -> None:
            store.pop("file_paths", None)
            selected.text = ""
            go_btn.disable()

        with ui.row().classes("gap-3"):
            go_btn = ui.button(
                "Go to Processing", on_click=lambda: ui.navigate.to("/processing")
            )
            if files:
                go_btn.enable()
            else:
                go_btn.disable()
            ui.button("Clear", on_click=clear).props("color=warning outline")


# ---------- Event bus and dummy engine ----------

EventType = Literal[
    "overall",  # overall counters
    "rule_started",  # a rule began
    "rule_status",  # short status line
    "rule_progress",  # 0.0-1.0 for the current rule
    "tool_call",  # tool name + safe args summary
    "tool_result",  # ok/error + brief summary
    "rule_completed",  # findings + ms
    "rule_failed",  # error case
    "done",  # all rules finished
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
    """Simulate a full rules run with rich events."""
    total = len(DUMMY_RULES)
    completed = 0
    total_findings = 0
    await emit(
        Event(
            "overall",
            data={"completed": completed, "total": total, "findings": total_findings},
        )
    )

    for idx, rule in enumerate(DUMMY_RULES, start=1):
        start_ms = time.perf_counter()
        rid = rule["id"]

        # start rule
        await emit(
            Event(
                "rule_started",
                rule_id=rid,
                data={"title": rule["title"], "tag": rule["tag"]},
            )
        )
        await emit(Event("rule_progress", rule_id=rid, data={"pct": 0.02}))
        await emit(
            Event("rule_status", rule_id=rid, data={"text": "Initializing datasets"})
        )
        await asyncio.sleep(0.2)

        # tool 1
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

        # tool 2
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

        # tool 3 (LLM scoring or policy engine)
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

        # complete rule
        findings = keep
        completed += 1
        total_findings += findings
        dur_ms = int((time.perf_counter() - start_ms) * 1000)
        await emit(
            Event(
                "rule_completed", rule_id=rid, data={"findings": findings, "ms": dur_ms}
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

        # tiny pause between rules
        await asyncio.sleep(0.15)

    await emit(Event("done"))


@ui.page("/processing")
async def processing_page() -> None:
    header()
    with ui.column().classes("w-full max-w-5xl mx-auto p-6 gap-4"):
        # ---------- Overall progress (only progress bar) ----------
        with ui.card().classes("w-full rounded-2xl"):
            with ui.row().classes("items-center justify-between w-full"):
                ui.label("Overall Progress").classes("text-lg font-semibold")
                overall_done = ui.label("0 of 0 completed").classes(
                    "text-sm text-gray-500"
                )
            overall_bar = ui.linear_progress(value=0.0).classes("w-full")
            with ui.row().classes("w-full gap-3"):
                with (
                    ui.card()
                    .props("flat bordered")
                    .classes("flex-1 items-center justify-center p-4")
                ):
                    overall_completed = ui.label("0").classes("text-3xl font-semibold")
                    ui.label("Rules Completed").classes("text-sm text-gray-600")
                with (
                    ui.card()
                    .props("flat bordered")
                    .classes("flex-1 items-center justify-center p-4")
                ):
                    overall_findings = ui.label("0").classes("text-3xl font-semibold")
                    ui.label("Total Findings").classes("text-sm text-gray-600")
                with (
                    ui.card()
                    .props("flat bordered")
                    .classes("flex-1 items-center justify-center p-4")
                ):
                    overall_percent = ui.label("0%").classes("text-3xl font-semibold")
                    ui.label("Progress").classes("text-sm text-gray-600")

        # ---------- Single current rule card (no progress bar) ----------
        with ui.card().classes("w-full rounded-2xl border"):
            with ui.row().classes("items-center justify-between w-full"):
                current_rule_title = ui.label("Waiting to start...").classes(
                    "text-base font-semibold"
                )
                current_rule_meta = ui.label("").classes(
                    "text-sm text-gray-500"
                )  # id · tag

            # Reserve vertical space for two lines so layout doesn't shift
            with ui.column().classes("w-full gap-1").style("min-height: 44px"):
                current_status = (
                    ui.label("Idle")
                    .classes("text-sm text-gray-600")
                    .style("min-height: 20px")
                )
                current_tool = (
                    ui.label(" ")
                    .classes("text-sm text-gray-600")
                    .style("min-height: 20px")
                )  # note: a single space

            # Scrolling activity feed
            rule_log = ui.log().classes("w-full h-56")

        with ui.row().classes("gap-3"):
            ui.button("Restart", on_click=lambda: ui.navigate.reload())
            next_btn = ui.button(
                "Continue to Report", on_click=lambda: ui.navigate.to("/report")
            )
            next_btn.disable()

        # ---------- Engine + event consumer ----------
        store = user_store()
        files = [Path(p) for p in (store.get("file_paths") or [])]
        if not files or not files[0].exists():
            ui.notify("No files selected. Please upload first.", type="warning")
            ui.navigate.to("/upload")
            return

        # start the dummy simulator; swap with your real agent later
        asyncio.create_task(run_engine(files))

        async def event_consumer() -> None:
            total = 0
            completed = 0
            findings_sum = 0
            current_rule_id = ""

            while True:
                ev = await EVENT_BUS.get()

                if ev.type == "overall":
                    data = ev.data or {}
                    completed = int(data.get("completed", completed))
                    total = int(data.get("total", total))
                    findings_sum = int(data.get("findings", findings_sum))
                    pct = completed / max(1, total)
                    overall_bar.value = pct
                    overall_completed.text = str(completed)
                    overall_findings.text = str(findings_sum)
                    overall_percent.text = f"{int(pct * 100)}%"
                    overall_done.text = f"{completed} of {total} completed"

                elif ev.type == "rule_started":
                    current_rule_id = ev.rule_id or ""
                    d = ev.data or {}
                    title = d.get("title", "")
                    tag = d.get("tag", "")
                    current_rule_title.text = title or "Running rule"
                    current_rule_meta.text = (
                        f"{current_rule_id} · {tag}" if tag else current_rule_id
                    )
                    current_status.text = "Starting"
                    current_tool.text = " "
                    rule_log.clear()
                    rule_log.push(f"Started {current_rule_meta.text}")

                elif ev.type == "rule_status":
                    d = ev.data or {}
                    msg = d.get("text", " ")
                    current_status.text = msg
                    rule_log.push(msg)

                # we intentionally ignore 'rule_progress' events to avoid a second bar

                elif ev.type == "tool_call":
                    d = ev.data or {}
                    name = d.get("name", "")
                    args = d.get("args", {})
                    current_tool.text = f"Tool: {name}"
                    rule_log.push(f"Tool {name} call {args}")

                elif ev.type == "tool_result":
                    d = ev.data or {}
                    name = d.get("name", " ")
                    ok = d.get("ok", True)
                    summary = d.get("summary", "")
                    status = "ok" if ok else "error"
                    rule_log.push(f"Tool {name} {status}: {summary}")
                    current_tool.text = ""

                elif ev.type == "rule_completed":
                    d = ev.data or {}
                    f = int(d.get("findings", 0))
                    ms = int(d.get("ms", 0))
                    current_status.text = f"Completed · {f} findings · {ms} ms"
                    rule_log.push(f"Completed {current_rule_id} with {f} findings")

                elif ev.type == "rule_failed":
                    current_status.text = "Failed"
                    rule_log.push(f"Failed {ev.rule_id}")

                elif ev.type == "done":
                    next_btn.enable()
                    current_status.text = "All rules finished"
                    rule_log.push("Run finished")
                    # keep consumer alive in case you run again via Restart

        asyncio.create_task(event_consumer())


@ui.page("/report")
def report_page() -> None:
    header()
    with ui.column().classes("max-w-4xl mx-auto p-6 gap-3"):
        ui.label("3) Report").classes("text-2xl font-bold")
        ui.label("Summary, metrics, and action items.").classes("text-gray-600")

        store = user_store()
        report = store.get("report")
        if not report:
            ui.notify("No report yet. Run processing first.", type="warning")
            ui.navigate.to("/processing")
            return

        with ui.card().classes("w-full"):
            ui.label("Summary").classes("text-lg font-semibold")
            ui.label(report["summary"]).classes("text-gray-700")

        with ui.card().classes("w-full"):
            ui.label("Metrics").classes("text-lg font-semibold")
            for k, v in report.get("metrics", {}).items():
                with ui.row().classes("justify-between"):
                    ui.label(k.capitalize())
                    ui.label(str(v))

        with ui.card().classes("w-full"):
            ui.label("Action Items").classes("text-lg font-semibold")
            for item in report.get("action_items", []):
                with ui.row().classes("justify-between items-center"):
                    ui.label(item["title"])
                    ui.label(f"Owner: {item['owner']} · Due: {item['due']}")

        def download_json() -> str:
            path = Path("./report.json")
            # JSON itself keeps its formatting; we do not insert newline escapes
            path.write_text(json.dumps(report, indent=2))
            return str(path)

        def download_csv() -> str:
            path = Path("./action_items.csv")
            rows = ["title,owner,due"] + [
                f"{i['title']},{i['owner']},{i['due']}" for i in report["action_items"]
            ]
            content = " EOL ".join(rows)
            path.write_text(content)
            return str(path)

        with ui.row().classes("gap-3"):
            with ui.row().classes("gap-3"):
                ui.button(
                    "Download JSON",
                    on_click=lambda: ui.download(
                        download_json(), filename="report.json"
                    ),
                )
                ui.button(
                    "Download Action Items CSV",
                    on_click=lambda: ui.download(
                        download_csv(), filename="action_items.csv"
                    ),
                )
                ui.button("Back to Upload", on_click=lambda: ui.navigate.to("/upload"))
            ui.button("Back to Upload", on_click=lambda: ui.navigate.to("/upload"))


@ui.page("/")
def index() -> None:
    ui.navigate.to("/upload")


if __name__ in {"__main__", "__mp_main__"}:
    app.add_static_files("/uploads", str(UPLOAD_DIR))
    ui.run(reload=True, port=8080, storage_secret=STORAGE_SECRET)
