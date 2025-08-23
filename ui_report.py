# ui_report.py
from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List

from nicegui import ui


# ---- helpers ---------------------------------------------------------------

SEV_COLOR = {
    "critical": "#ef4444",  # red-500
    "high": "#f97316",  # orange-500
    "medium": "#f59e0b",  # amber-500
    "low": "#10b981",  # emerald-500
}

KANBAN_COLUMNS = [
    ("new", "New Leads"),
    ("in_progress", "In Progress"),
    ("review", "Pending Review"),
    ("resolved", "Resolved"),
    ("suspicious", "Suspicious Fraud"),
    ("compliance", "Compliance Issue"),
]


@dataclass
class Case:
    id: str
    title: str
    amount: str
    risk: str  # critical | high | medium | low
    status: str  # one of KANBAN_COLUMNS keys
    badge: str  # label to show on the card


def _seed_cases_from_report(report: Dict) -> List[Dict]:
    """Create a few demo cases from the mocked audit findings."""
    rnd = random.Random(42)
    cases: List[Case] = []

    findings = report.get("raw", {}).get("findings", [])
    if not findings:
        # Fallback: fabricate 6 cards if nothing is in report
        findings = [
            {"test": "GST Compliance", "severity": "high", "count": 5},
            {"test": "Journal Entries", "severity": "medium", "count": 3},
            {"test": "Bank Reconciliation", "severity": "high", "count": 2},
            {"test": "Fixed Assets", "severity": "medium", "count": 4},
        ]

    # pick up to ~8 cases from findings with count>0
    for i, f in enumerate(findings):
        if f.get("count", 0) <= 0:
            continue
        for n in range(min(2, int(f["count"]))):
            amt_k = rnd.choice([45, 80, 120, 250, 350])
            status = KANBAN_COLUMNS[min(n, len(KANBAN_COLUMNS) - 1)][0]  # spread a bit
            cases.append(
                Case(
                    id=f"{f['test'][:3].upper()}-{i:03d}-{n}",
                    title=f.get("test", "Finding"),
                    amount=f"₹{amt_k}K",
                    risk=f.get("severity", "medium").lower(),
                    status=status,
                    badge="High Risk"
                    if f.get("severity", "").lower() in {"high", "critical"}
                    else "Medium Risk",
                ).__dict__
            )
            if len(cases) >= 8:
                break
        if len(cases) >= 8:
            break

    return cases


def _treemap_options_from_report(report: Dict) -> Dict:
    """Build a simple ECharts treemap options dict."""
    findings = report.get("raw", {}).get("findings", [])
    data = []
    for f in findings:
        name = f.get("test", "Unknown")
        value = max(1, int(f.get("count", 0)))
        sev = f.get("severity", "medium").lower()
        color = SEV_COLOR.get(sev, "#9ca3af")  # slate-400 default
        data.append(
            {
                "name": name,
                "value": value,
                "itemStyle": {"color": color},
            }
        )

    if not data:
        data = [{"name": "No Findings", "value": 1, "itemStyle": {"color": "#9ca3af"}}]

    return {
        "tooltip": {"trigger": "item"},
        "series": [
            {
                "type": "treemap",
                "roam": False,
                "nodeClick": False,
                "breadcrumb": {"show": False},
                "label": {"show": True, "overflow": "truncate"},
                "data": data,
            }
        ],
    }


# ---- page registration -----------------------------------------------------


def register_report_page(
    header: Callable[[], None],
    user_store: Callable[[], dict],
) -> None:
    @ui.page("/report")
    def report_page() -> None:  # noqa: F811
        header()
        # Ensure q-card inner sections have no default padding when desired
        ui.add_head_html(
            "<style>.card-zero-pad .q-card__section{padding:0!important;margin:0!important}</style>"
        )

        store = user_store()
        report = store.get("report")
        if not report:
            ui.notify("No report yet. Run processing first.", type="warning")
            ui.navigate.to("/processing")
            return

        with ui.column().classes("w-full max-w-5xl mx-auto p-6 gap-4"):
            # ---------- Summary (top) ----------
            with ui.row().classes("items-center justify-between w-full"):
                ui.label("Investigation Dashboard · AI Audit Results").classes(
                    "text-xl font-semibold"
                )

            # Removed summary text display per request

            with ui.row().classes("w-full gap-3"):
                with (
                    ui.card()
                    .props("flat bordered")
                    .classes("flex-1 items-center justify-center p-4")
                ):
                    ui.label(
                        str(report.get("metrics", {}).get("rules_total", 0))
                    ).classes("text-3xl font-semibold")
                    ui.label("Rules Executed").classes("text-sm text-gray-600")
                with (
                    ui.card()
                    .props("flat bordered")
                    .classes("flex-1 items-center justify-center p-4")
                ):
                    ui.label(str(report.get("metrics", {}).get("findings", 0))).classes(
                        "text-3xl font-semibold"
                    )
                    ui.label("Total Findings").classes("text-sm text-gray-600")
                with (
                    ui.card()
                    .props("flat bordered")
                    .classes("flex-1 items-center justify-center p-4")
                ):
                    high = report.get("metrics", {}).get("critical", 0) + report.get(
                        "metrics", {}
                    ).get("high", 0)
                    ui.label(str(high)).classes("text-3xl font-semibold")
                    ui.label("High-Risk Findings").classes("text-sm text-gray-600")

            # ---------- Visualization (middle) ----------
            # Remove padding around the chart while keeping tidy padding for text/legend
            with ui.card().classes("w-full p-0 card-zero-pad"):
                with ui.column().classes("p-4 pb-2"):
                    ui.label("Compliance Risk Analysis").classes(
                        "text-lg font-semibold"
                    )
                    ui.label(
                        "Size indicates anomaly count, color shows risk level."
                    ).classes("text-sm text-gray-600")
                ui.echart(_treemap_options_from_report(report)).classes(
                    "w-full p-0 -mt-6 -mb-6"
                ).style(
                    "height: 360px; margin-top: -24px; margin-bottom: -24px; padding: 0"
                )

                with ui.row().classes("items-center gap-4 p-4 pt-2"):
                    # legend
                    for label, color in [
                        ("High Risk", SEV_COLOR["high"]),
                        ("Medium Risk", SEV_COLOR["medium"]),
                        ("Low Risk", SEV_COLOR["low"]),
                    ]:
                        with ui.row().classes("items-center gap-2"):
                            ui.element("span").style(
                                f"display:inline-block;width:10px;height:10px;border-radius:9999px;background:{color}"
                            )
                            ui.label(label).classes("text-sm text-gray-600")

                        # Back and CSV buttons (bottom)
                        # ================== Kanban (4 columns, button -> menu to move) ==================
            COLUMNS = ["Issues", "In Progress", "Pending Review", "Resolved"]

            # Seed simple cases from the report if none in session
            # Normalize any previously stored cases from older schema
            def _seed_cases(report: dict, limit: int = 6) -> list[dict]:
                cases: list[dict] = []
                i = 1
                # prefer raw.findings if available (current report shape)
                for f in report.get("raw", {}).get("findings", []) or report.get(
                    "findings", []
                ):
                    count = int(f.get("count", 0))
                    if count <= 0:
                        continue
                    for _ in range(min(count, 2)):  # up to 2 cards per finding
                        cases.append(
                            {
                                "id": f"{f.get('test', 'CASE')[:3].upper()}-{i:03d}",
                                "title": f.get("test", "Investigation"),
                                "risk": f.get("severity", "medium").title(),
                                "amount": "₹" + str(50 * i) + "K",
                                "assignee": "Unassigned",
                                "status": "Issues",  # all start in Issues
                            }
                        )
                        i += 1
                        if len(cases) >= limit:
                            return cases
                # fallback if no findings
                if not cases:
                    for j in range(1, limit + 1):
                        cases.append(
                            {
                                "id": f"CASE-{j:03d}",
                                "title": "Review anomaly",
                                "risk": "Medium",
                                "amount": f"₹{80 + 10 * j}K",
                                "assignee": "Unassigned",
                                "status": "Issues",
                            }
                        )
                return cases

            # session-backed cases
            status_map = {
                "new": "Issues",
                "in_progress": "In Progress",
                "review": "Pending Review",
                "resolved": "Resolved",
                # collapse extra statuses into closest columns
                "suspicious": "Issues",
                "compliance": "Pending Review",
            }

            cases = store.get("cases")
            if not cases:
                cases = _seed_cases(report)
            else:
                # migrate any old statuses to current column names
                for c in cases:
                    s = (c.get("status") or "").strip()
                    c["status"] = status_map.get(s, s if s in COLUMNS else "Issues")
            store["cases"] = cases

            # board view and containers
            board: dict[str, list[dict]] = {
                c: [x for x in cases if x["status"] == c] for c in COLUMNS
            }
            col_boxes: dict[str, ui.column] = {}
            col_counts: dict[str, ui.label] = {}

            def move_case(case_id: str, dest: str) -> None:
                if dest not in COLUMNS:
                    return
                # update session model
                for x in store["cases"]:
                    if x["id"] == case_id:
                        old = x["status"]
                        x["status"] = dest
                        # update board view
                        if x in board.get(old, []):
                            board[old].remove(x)
                        board[dest].append(x)
                        break
                render_board()

            def risk_badge(risk: str) -> ui.badge:
                risk_l = (risk or "Medium").lower()
                color = (
                    "negative"
                    if risk_l in {"high", "critical"}
                    else ("warning" if risk_l == "medium" else "positive")
                )
                return ui.badge(f"{risk.title()} risk").props(f"color={color} outline")

            def render_board() -> None:
                for col in COLUMNS:
                    box = col_boxes[col]
                    box.clear()
                    col_counts[col].text = str(len(board[col]))
                    # ensure cards are created inside the column container
                    with box:
                        for c in board[col]:
                            # Compact, outlined item cards with slight radius and gray border
                            item_card = (
                                ui.card()
                                .props("flat bordered")
                                .classes(
                                    "w-full mb-0 rounded border border-gray-200 bg-white p-3"
                                )
                            )
                            with item_card:
                                with ui.column().classes("w-full gap-1"):
                                    ui.label(c["id"]).classes(
                                        "text-[11px] text-gray-500"
                                    )
                                    ui.label(c["title"]).classes(
                                        "text-sm font-medium"
                                    )
                                    with ui.row().classes(
                                        "items-center justify-between w-full mt-0.5"
                                    ):
                                        with ui.row().classes("items-center gap-2"):
                                            risk_badge(c["risk"])
                                            ui.label(c["amount"]).classes(
                                                "text-xs text-gray-500"
                                            )
                                        # Move menu
                                        with ui.button("Move").props(
                                            "flat dense"
                                        ) as btn:
                                            pass
                                        with ui.menu().props(
                                            'anchor="bottom right" self="top right"'
                                        ):
                                            for dest in COLUMNS:
                                                ui.menu_item(
                                                    dest,
                                                    on_click=lambda _=None,
                                                    cid=c["id"],
                                                    d=dest: move_case(cid, d),
                                                )

            with ui.card().classes("w-full rounded-2xl"):
                with ui.row().classes("items-center justify-between w-full"):
                    ui.label("Investigation Kanban Board").classes(
                        "text-lg font-semibold"
                    )
                    # counts header (Issues has initial 6 in your example; values update live)
                    ui.label(
                        f"Issues {len(board['Issues'])}  ·  In Progress {len(board['In Progress'])}  ·  Pending Review {len(board['Pending Review'])}  ·  Resolved {len(board['Resolved'])}"
                    ).classes("text-xs text-gray-500")

                with ui.row().classes("w-full gap-4"):
                    for col in COLUMNS:
                        # Faint background per column + outlined container
                        bg_map = {
                            "Issues": "bg-blue-50",
                            "In Progress": "bg-yellow-50",
                            "Pending Review": "bg-purple-50",
                            "Resolved": "bg-green-50",
                        }
                        border_map = {
                            "Issues": "border-blue-200",
                            "In Progress": "border-yellow-200",
                            "Pending Review": "border-purple-200",
                            "Resolved": "border-green-200",
                        }
                        column_card = (
                            ui.card()
                            .props("flat bordered")
                            .classes(
                                f"flex-1 rounded-xl border {border_map.get(col, 'border-gray-300')} {bg_map.get(col, '')}"
                            )
                        )
                        with column_card:
                            with ui.row().classes("items-center justify-between"):
                                ui.label(col).classes("text-sm font-semibold")
                                col_counts[col] = ui.label(
                                    str(len(board[col]))
                                ).classes("text-xs text-gray-500")
                            col_boxes[col] = ui.column().classes("mt-2")

                render_board()

            with ui.row().classes("gap-3"):
                ui.button(
                    "Download Action Items CSV",
                    on_click=lambda: ui.download(
                        _download_csv(report), filename="action_items.csv"
                    ),
                )
                ui.button("Back to Upload", on_click=lambda: ui.navigate.to("/upload"))


def _download_json(report: Dict) -> str:
    path = Path("./report.json")
    path.write_text(json.dumps(report, indent=2))
    return str(path)


def _download_csv(report: Dict) -> str:
    path = Path("./action_items.csv")
    rows = ["title,owner,due"] + [
        f"{i['title']},{i['owner']},{i['due']}" for i in report.get("action_items", [])
    ]
    content = " EOL ".join(rows)
    path.write_text(content)
    return str(path)
