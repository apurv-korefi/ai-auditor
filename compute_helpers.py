# compute_helpers.py
from __future__ import annotations

from typing import List, Tuple
import pandas as pd


def je_same_user_post_approve(
    df: pd.DataFrame,
    id_col: str = "je_id",
    posted_by_col: str = "posted_by",
    approved_by_col: str = "approved_by",
) -> Tuple[int, List[str]]:
    """Return (count, sample_ids) where poster == approver."""
    mask = (
        df[posted_by_col].astype(str).str.lower()
        == df[approved_by_col].astype(str).str.lower()
    )
    hits = df[mask]
    sample = hits[id_col].astype(str).head(10).tolist()
    return len(hits), sample


def p2p_duplicate_invoices(
    df: pd.DataFrame,
    vendor_col: str = "vendor_id",
    inv_col: str = "invoice_no",
    amt_col: str = "amount",
) -> Tuple[int, List[str]]:
    """Duplicate groups by (vendor_id, invoice_no, amount)."""
    grp = (
        df.groupby([vendor_col, inv_col, amt_col], dropna=False)
        .size()
        .reset_index(name="n")
    )
    dups = grp[grp["n"] > 1]
    ids = (
        df.merge(
            dups[[vendor_col, inv_col, amt_col]], on=[vendor_col, inv_col, amt_col]
        )[inv_col]
        .astype(str)
        .unique()
        .tolist()[:10]
    )
    return len(dups), ids


def fictitious_vendors(
    vendors: pd.DataFrame,
    employees: pd.DataFrame,
    v_addr: str = "address",
    e_addr: str = "address",
    v_id: str = "vendor_id",
) -> Tuple[int, List[str]]:
    """Address collisions between vendors and employees."""
    v = vendors.copy()
    e = employees.copy()
    v[v_addr] = v[v_addr].astype(str).str.strip().str.lower()
    e[e_addr] = e[e_addr].astype(str).str.strip().str.lower()
    matches = v.merge(e, left_on=v_addr, right_on=e_addr, how="inner")
    sample = matches[v_id].astype(str).head(10).tolist()
    return len(matches), sample


def terminated_users_with_access(
    user_access: pd.DataFrame,
    employees: pd.DataFrame,
    user_id: str = "user_id",
    status_col: str = "employment_status",
    active_flag: str = "active",
) -> Tuple[int, List[str]]:
    """Terminated employees who still have active access."""
    term = employees[employees[status_col].astype(str).str.lower().eq("terminated")]
    merged = user_access.merge(term[[user_id]], on=user_id, how="inner")
    still_active = merged[merged[active_flag] == True]
    sample = still_active[user_id].astype(str).head(10).tolist()
    return len(still_active), sample
