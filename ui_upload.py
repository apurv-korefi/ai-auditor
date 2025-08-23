# ui_upload.py
from __future__ import annotations

from pathlib import Path
from typing import Callable, List

from nicegui import ui
from nicegui.events import UploadEventArguments


def register_upload_page(
    header: Callable[[], None],
    user_store: Callable[[], dict],
    upload_dir: Path,
) -> None:
    """Register the /upload page; supports multi-file upload and enables the next step only after files are present."""

    @ui.page("/upload")
    def upload_page() -> None:  # noqa: F811
        header()

        with ui.column().classes("w-full max-w-5xl mx-auto p-6 bg-slate-50 gap-3"):
            ui.label("Upload").classes("text-2xl font-bold")
            ui.label("Choose one or more files to process.").classes("text-gray-600")

            store = user_store()
            selected = ui.label().classes("text-sm text-gray-500")

            files: List[str] = list(store.get("file_paths") or [])
            if files:
                selected.text = f"Selected: {len(files)} file(s)"

            def on_upload(e: UploadEventArguments) -> None:
                dest = upload_dir / e.name
                dest.write_bytes(e.content.read())
                file_paths = list(store.get("file_paths") or [])
                file_paths.append(str(dest))
                store["file_paths"] = file_paths
                selected.text = f"Selected: {len(file_paths)} file(s)"
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
