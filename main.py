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


def user_store() -> dict:
    """Per-session storage on server side; requires storage_secret in ui.run."""
    return app.storage.user


def header() -> None:
    ui.colors(primary="#0f172a")  # Tailwind slate-900
    with ui.header().classes("bg-primary"):  # use brand color
        with ui.row().classes("w-full justify-center"):  # centers the inner container
            with ui.row().classes("w-full max-w-5xl items-center justify-between px-4"):
                ui.label(APP_TITLE).classes("text-xl font-semibold text-white")
                # put nav/actions here later if you want; if empty, the title still sits in the centered container


from ui_upload import register_upload_page

register_upload_page(header, user_store, UPLOAD_DIR)

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
