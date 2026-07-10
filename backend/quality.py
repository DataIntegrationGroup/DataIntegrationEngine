# ===============================================================================
# Copyright 2024 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
"""Normalize each provider's raw approval/status value into a common vocabulary.

Sources report data status in incompatible ways — USGS "Approved"/"Provisional",
WQP "Historical"/"Accepted"/"Preliminary", NMBGMR AMP a PublicRelease boolean,
BOR a RISE ``status`` string. The raw value is preserved on the record
(``approval_status``); this maps it to one of a small set of terms
(``approval_status_normalized``) so a consumer can filter "provisional vs
approved" across every source uniformly.
"""

APPROVED = "approved"
PROVISIONAL = "provisional"
UNKNOWN = "unknown"

# Provider terms that mean finalized / vetted / released data.
_APPROVED_TERMS = frozenset(
    {
        "approved",
        "accepted",
        "final",
        "finalized",
        "validated",
        "verified",
        "historical",
        "published",
        "reviewed",
        "public release",
        "released",
    }
)

# Provider terms that mean not-yet-finalized / revisable data.
_PROVISIONAL_TERMS = frozenset(
    {
        "provisional",
        "preliminary",
        "working",
        "estimated",
        "unverified",
        "unvalidated",
        "raw",
        "draft",
        "in review",
        "tentative",
        "not released",
    }
)


def normalize_approval_status(raw) -> str:
    """Map a provider's raw approval/status value to APPROVED / PROVISIONAL /
    UNKNOWN. ``None`` / "" → UNKNOWN. A boolean is treated as a release flag
    (True → approved, False → provisional; e.g. NMBGMR AMP's PublicRelease)."""
    if raw is None or raw == "":
        return UNKNOWN
    if isinstance(raw, bool):
        return APPROVED if raw else PROVISIONAL

    s = str(raw).strip().lower()
    if s in _APPROVED_TERMS:
        return APPROVED
    if s in _PROVISIONAL_TERMS:
        return PROVISIONAL
    # substring fallback for compound strings ("provisional data", ...);
    # check provisional first so "unapproved/provisional" is not read as approved.
    if any(t in s for t in _PROVISIONAL_TERMS):
        return PROVISIONAL
    if any(t in s for t in _APPROVED_TERMS):
        return APPROVED
    return UNKNOWN
