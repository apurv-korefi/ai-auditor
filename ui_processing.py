# ui_processing.py
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Callable, Optional

from nicegui import ui
from processing_runtime import EVENT_BUS, run_agent




def register_processing_page(
    header: Callable[[], None],
    user_store: Callable[[], dict],
) -> None:
    @ui.page("/processing")
    async def processing_page() -> None:  # noqa: F811
        header()

        with ui.column().classes("w-full max-w-5xl mx-auto p-6 gap-4"):
            # Top card with single progress bar
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
                        overall_completed = ui.label("0").classes(
                            "text-3xl font-semibold"
                        )
                        ui.label("Rules Completed").classes("text-sm text-gray-600")
                    with (
                        ui.card()
                        .props("flat bordered")
                        .classes("flex-1 items-center justify-center p-4")
                    ):
                        overall_findings = ui.label("0").classes(
                            "text-3xl font-semibold"
                        )
                        ui.label("Total Findings").classes("text-sm text-gray-600")
                    with (
                        ui.card()
                        .props("flat bordered")
                        .classes("flex-1 items-center justify-center p-4")
                    ):
                        overall_percent = ui.label("0%").classes(
                            "text-3xl font-semibold"
                        )
                        ui.label("Progress").classes("text-sm text-gray-600")

            # Single current rule card (no rule progress bar)
            with ui.card().classes("w-full rounded-2xl border"):
                with ui.row().classes("items-center justify-between w-full"):
                    current_rule_title = ui.label("Waiting to start...").classes(
                        "text-base font-semibold"
                    )
                    current_rule_meta = ui.label("").classes("text-sm text-gray-500")
                # fixed height area to avoid layout jumps
                with ui.column().classes("w-full gap-1").style("min-height: 44px"):
                    current_status = (
                        ui.label("Starting soon")
                        .classes("text-sm text-gray-600")
                        .style("min-height: 20px")
                    )
                    current_tool = (
                        ui.label(" ")
                        .classes("text-sm text-gray-600")
                        .style("min-height: 20px")
                    )
                # scrolling activity feed
                rule_log = ui.log().classes("w-full h-56")

            # Buttons (wire cleanup on navigate)
            with ui.row().classes("gap-3"):
                restart_btn = ui.button("Restart")
                next_btn = ui.button("Continue to Report")
                next_btn.disable()

        # ---------- Start engine and consume events ----------
        store = user_store()
        files = [Path(p) for p in (store.get("file_paths") or [])]
        if not files or not files[0].exists():
            ui.notify("No files selected. Please upload first.", type="warning")
            ui.navigate.to("/upload")
            return

        engine_task: Optional[asyncio.Task] = asyncio.create_task(run_agent(files))

        async def event_consumer() -> None:
            try:
                total = 0
                completed = 0
                findings_sum = 0
                current_rule_id: Optional[str] = ""

                while True:
                    ev = await EVENT_BUS.get()

                    if ev.type == "overall":
                        d = ev.data or {}
                        completed = int(d.get("completed", completed))
                        total = int(d.get("total", total))
                        findings_sum = int(d.get("findings", findings_sum))
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
                        try:
                            rule_log.push(f"Started {current_rule_meta.text}")
                        except RuntimeError:
                            return  # client gone

                    elif ev.type == "rule_status":
                        d = ev.data or {}
                        msg = d.get("text", " ")
                        current_status.text = msg
                        try:
                            rule_log.push(msg)
                        except RuntimeError:
                            return

                    # We ignore rule_progress events (only one progress bar at top)

                    elif ev.type == "tool_call":
                        d = ev.data or {}
                        name = d.get("name", "")
                        args = d.get("args", {})
                        current_tool.text = f"Tool: {name}"
                        try:
                            rule_log.push(f"Tool {name} call {args}")
                        except RuntimeError:
                            return

                    elif ev.type == "tool_result":
                        d = ev.data or {}
                        name = d.get("name", " ")
                        ok = d.get("ok", True)
                        summary = d.get("summary", "")
                        status = "ok" if ok else "error"
                        try:
                            rule_log.push(f"Tool {name} {status}: {summary}")
                        except RuntimeError:
                            return
                        current_tool.text = " "

                    elif ev.type == "rule_completed":
                        d = ev.data or {}
                        f = int(d.get("findings", 0))
                        ms = int(d.get("ms", 0))
                        current_status.text = f"Completed · {f} findings · {ms} ms"
                        try:
                            rule_log.push(
                                f"Completed {current_rule_id} with {f} findings"
                            )
                        except RuntimeError:
                            return

                    elif ev.type == "rule_failed":
                        d = ev.data or {}
                        err = d.get("error", "")
                        current_status.text = "Failed"
                        try:
                            rule_log.push(
                                f"Failed {ev.rule_id}: {err}" if err else f"Failed {ev.rule_id}"
                            )
                        except RuntimeError:
                            return

                    elif ev.type == "done":
                        # expect a real report from engine
                        if ev.data and ev.data.get("report"):
                            store["report"] = ev.data.get("report")
                            next_btn.enable()
                            current_status.text = "All rules finished"
                        else:
                            current_status.text = "Finished without report"
                            ui.notify(
                                "Run finished but no report was returned.",
                                type="warning",
                            )
                        try:
                            rule_log.push("Run finished")
                        except RuntimeError:
                            pass
            except asyncio.CancelledError:
                return  # exit quietly when we cancel on navigation

        consumer_task: asyncio.Task = asyncio.create_task(event_consumer())

        # Wire buttons with cleanup
        def cleanup_tasks() -> None:
            if not engine_task.done():
                engine_task.cancel()
            if not consumer_task.done():
                consumer_task.cancel()

        restart_btn.on_click(lambda: (cleanup_tasks(), ui.navigate.reload()))
        next_btn.on_click(lambda: (cleanup_tasks(), ui.navigate.to("/report")))
