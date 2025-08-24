# compute_helpers.py
from __future__ import annotations

from typing import List, Optional
import pandas as pd
from pydantic import BaseModel


# ---------------- Pydantic models (shared with tools/agent) ----------------


class Finding(BaseModel):
    test: str
    severity: str
    count: int
    sample_ids: List[str]
    notes: Optional[str] = None


class AuditReport(BaseModel):
    findings: List[Finding]
    summary: str


# ---------------- Pure compute helpers (no side effects) ----------------


def compute_je_same_user_post_approve(
    df: pd.DataFrame,
    id_col: str = "je_id",
    posted_by_col: str = "posted_by",
    approved_by_col: str = "approved_by",
) -> Finding:
    """
    Flag journal entries where the same user posted and approved.
    """
    lhs = df[posted_by_col].astype(str).str.lower()
    rhs = df[approved_by_col].astype(str).str.lower()
    hits = df[lhs.eq(rhs)]
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
    """
    Detect duplicate invoices defined by identical (vendor_id, invoice_no, amount).
    """
    grp = (
        df.groupby([vendor_col, inv_col, amt_col], dropna=False)
        .size()
        .reset_index(name="n")
    )
    dups = grp[grp["n"] > 1]

    # Collect a small set of sample invoice ids involved in duplicates
    ids = (
        df.merge(
            dups[[vendor_col, inv_col, amt_col]], on=[vendor_col, inv_col, amt_col]
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
    vendors: pd.DataFrame,
    employees: pd.DataFrame,
    v_addr: str = "address",
    e_addr: str = "address",
    v_id: str = "vendor_id",
) -> Finding:
    """
    Identify vendors whose (normalized) address matches an employee address.
    """
    v = vendors.copy()
    e = employees.copy()

    def _norm(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.lower()

    v[v_addr] = _norm(v[v_addr])
    e[e_addr] = _norm(e[e_addr])

    matches = v.merge(e, left_on=v_addr, right_on=e_addr, how="inner")
    sample = matches[v_id].astype(str).head(10).tolist()

    return Finding(
        test="Fictitious vendor (address match)",
        severity="medium",
        count=len(matches),
        sample_ids=sample,
    )


def compute_terminated_users_with_access(
    user_access: pd.DataFrame,
    employees: pd.DataFrame,
    user_id: str = "user_id",
    status_col: str = "employment_status",
    active_flag: str = "active",
) -> Finding:
    """
    Terminated employees who still have active access in a permissions table.
    """
    term = employees[employees[status_col].astype(str).str.lower().eq("terminated")]
    merged = user_access.merge(term[[user_id]], on=user_id, how="inner")
    # Keep strictly True; if your data stores "Y"/"N", map before calling this function.
    still_active = merged[merged[active_flag] == True]  # noqa: E712

    sample = still_active[user_id].astype(str).head(10).tolist()

    return Finding(
        test="Terminated users with access",
        severity="critical",
        count=len(still_active),
        sample_ids=sample,
    )


__all__ = [
    "Finding",
    "AuditReport",
    "compute_je_same_user_post_approve",
    "compute_p2p_duplicate_invoices",
    "compute_fictitious_vendors",
    "compute_terminated_users_with_access",
]
