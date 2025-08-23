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


# after you define header() and user_store(), import and register:
from ui_processing import register_processing_page

register_processing_page(header, user_store)


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
