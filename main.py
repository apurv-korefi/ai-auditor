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

APP_TITLE = "AI Audit Assitant"
UPLOAD_DIR = Path("./uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Random secret is fine for local runs; you can also set NICEGUI_STORAGE_SECRET in env
STORAGE_SECRET = os.getenv("NICEGUI_STORAGE_SECRET", secrets.token_urlsafe(32))

# Dark mode controller (global)
DARK = ui.dark_mode()


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
    with ui.header().classes("items-center justify-between px-4 bg-slate-900"):
        with ui.column().classes("max-w-6xl"):
            ui.label(APP_TITLE).classes("text-xl font-semibold")


@ui.page("/upload")
def upload_page() -> None:
    header()
    with ui.column().classes("max-w-5xl mx-auto p-6 gap-3"):
        ui.label("1) Upload").classes("text-2xl font-bold")
        ui.label("Choose a file to process.").classes("text-gray-600")

        store = user_store()
        selected = ui.label().classes("text-sm text-gray-500")

        # If a file was uploaded earlier in this session, reflect it and allow proceeding
        existing = store.get("file_path")
        if existing:
            selected.text = f"Selected: {Path(existing).name}"

        def on_upload(e: UploadEventArguments) -> None:
            dest = UPLOAD_DIR / e.name
            dest.write_bytes(e.content.read())
            store["file_path"] = str(dest)
            selected.text = f"Selected: {dest.name}"
            go_btn.enable()
            ui.notify(f"Uploaded {dest.name}", type="positive")

        ui.upload(on_upload=on_upload, auto_upload=True).props(
            "accept=*/*, max-files=1"
        )

        def clear() -> None:
            store.pop("file_path", None)
            selected.text = ""
            go_btn.disable()

        with ui.row().classes("gap-3"):
            go_btn = ui.button(
                "Go to Processing", on_click=lambda: ui.navigate.to("/processing")
            )
            if existing:
                go_btn.enable()
            else:
                go_btn.disable()
            ui.button("Clear", on_click=clear).props("color=warning outline")


@ui.page("/processing")
async def processing_page() -> None:
    header()
    with ui.column().classes("max-w-4xl mx-auto p-6 gap-3"):
        ui.label("2) Processing").classes("text-2xl font-bold")
        ui.label("Streaming agent output in real-time.").classes("text-gray-600")

        store = user_store()
        file_path = Path(store.get("file_path", "")) if store.get("file_path") else None
        if not file_path or not file_path.exists():
            ui.notify("No file selected. Please upload a file first.", type="warning")
            ui.navigate.to("/upload")
            return

        with ui.row().classes("items-center gap-2"):
            ui.icon("description")
            ui.label(file_path.name).classes("text-sm text-gray-500")

        stream_box = (
            ui.textarea(label="Agent stream", placeholder="Waiting for tokens...")
            .props("rows=12, readonly")
            .classes("w-full font-mono")
        )

        progress = ui.linear_progress(show_value=False).props("instant-feedback")

        with ui.row().classes("gap-3"):
            ui.button("Restart", on_click=lambda: ui.navigate.reload())
            navigate_btn = ui.button(
                "Continue to Report", on_click=lambda: ui.navigate.to("/report")
            )
            navigate_btn.disable()

        async def run_stream() -> None:
            progress.value = 0.2
            stream_box.value = ""
            async for token in fake_agent_stream(file_path):
                stream_box.value += token
                await asyncio.sleep(0)
            progress.value = 0.9
            report = build_report(file_path)
            store["report"] = report
            progress.value = 1.0
            navigate_btn.enable()
            ui.notify("Processing complete", type="positive")

        ui.timer(0.2, run_stream, once=True)


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
    ui.run(reload=False, port=8080, storage_secret=STORAGE_SECRET)
