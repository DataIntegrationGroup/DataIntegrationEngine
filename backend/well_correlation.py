# ===============================================================================
# Copyright 2025 Jake Ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
"""Cross-agency well correlation.

Every agency (NMBGMR, USGS-NWIS, OSE ISC Seven Rivers, PVACD, ...) assigns its
own identifier to a physical well, and the same well is often monitored by more
than one agency. A few wells carry explicit cross-references
(``alternate_site_id`` / ``usgs_site_id``), but most do not. This module links
wells that are the *same physical well* across agencies so every well can be
traced back — ideally to an OSE Point of Diversion (POD).

Two kinds of evidence link wells:

1. **Explicit id references** (high confidence). A site's ``usgs_site_id`` or the
   tokens in its ``alternate_site_id`` are matched against the ids (and USGS ids)
   of every other site. A match is a hard link — the agencies themselves assert
   the wells are the same.

2. **Spatial + attribute agreement** (lower confidence). OSE PODs and many older
   records are not accurately surveyed, so latitude/longitude cannot be trusted
   as the exact well position. Two wells from *different* agencies within
   ``max_link_distance_m`` of each other are candidate matches; the confidence is
   raised when their reported well depths agree (``depth_tolerance_ft``) and
   lowered when depth is unavailable to corroborate.

Links are unioned into connected components — each component is one inferred
physical well. Every component is then associated with any OSE POD(s) it
contains or that sit within ``pod_link_distance_m`` of it.

:func:`correlate_wells` returns one crosswalk record per *input* well (every
well is emitted, matched or not), carrying the ids of the associated wells from
other agencies and the linked OSE POD(s), plus the method and a confidence.

The algorithm is pure and deterministic: given the same sites it always returns
the same crosswalk, independent of input order. It depends only on the standard
library so it is cheap to unit-test in isolation from the connectors.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections import defaultdict
from typing import Any, Iterable, Optional

# Default correlation parameters. Latitude/longitude are not trustworthy to the
# meter for these sources, so the spatial thresholds are deliberately generous;
# tighten them via the keyword arguments to correlate_wells.
DEFAULT_MAX_LINK_DISTANCE_M = 150.0
DEFAULT_DEPTH_TOLERANCE_FT = 50.0
DEFAULT_ELEVATION_TOLERANCE_FT = 20.0
DEFAULT_POD_LINK_DISTANCE_M = 150.0
DEFAULT_POD_SOURCE = "NMOSEPOD"

# Confidence score for an explicit id reference (the agencies assert the match).
_CONF_EXPLICIT = 0.95

# Spatial confidence is built up from corroborating attributes rather than being
# a fixed value: proximity alone is weak, and each independent attribute that
# agrees (depth, elevation, name/id tokens) raises confidence toward — but never
# to — the explicit level. A single disagreeing attribute (depth or elevation
# out of tolerance) rejects the pair outright: same location + clearly different
# construction means stacked/adjacent wells, not one well.
_CONF_SPATIAL_BASE = 0.35  # proximity within threshold, nothing else known
_CONF_BONUS_CLOSE = 0.10  # within 1/3 of the distance threshold
_CONF_BONUS_DEPTH = 0.25  # reported well depths agree
_CONF_BONUS_ELEVATION = 0.12  # reported surface elevations agree
_CONF_BONUS_NAME = 0.10  # name / id share a token
_CONF_SPATIAL_CAP = 0.9  # spatial evidence never reaches explicit (0.95)

# Prefixes stripped when normalizing an identifier for cross-reference matching
# (e.g. NWIS ids are "USGS-08313000" but NMBGMR records the bare "08313000").
_ID_PREFIXES = ("USGS-", "USGS:", "NWIS-", "NWIS:")

_EARTH_RADIUS_M = 6371008.8

CORRELATION_METHOD_DESCRIPTION = (
    "Cross-agency well correlation: wells are linked by explicit id references "
    "(usgs_site_id / alternate_site_id, confidence 0.95) — including USGS "
    "15-digit station numbers transcribed with a space or embedded in a site "
    "name, which are normalized to the canonical NWIS id — and by spatial "
    "proximity between agencies (default <=150 m). Spatial confidence starts low "
    "(0.35) and is raised by each corroborating attribute that agrees — well "
    "depth (<=50 ft), surface elevation (<=20 ft), a shared name/id token, and "
    "very close proximity — up to a 0.9 cap; a depth or elevation that disagrees "
    "beyond tolerance rejects the pair. Linked wells are unioned into components "
    "(one inferred physical well) and associated with any OSE POD they contain "
    "or that lies within the spatial threshold. Coordinates are approximate."
)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _num(value: Any) -> Optional[float]:
    """Coerce to float, or None when missing/unparseable."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_id(value: Any) -> str:
    """Uppercase, strip surrounding whitespace, and collapse internal spaces.
    Returns "" for missing values."""
    if value is None:
        return ""
    s = str(value).strip().upper()
    return " ".join(s.split())


def _match_keys(value: Any) -> set[str]:
    """The set of normalized forms an identifier can be matched by: the
    normalized value itself and, if it carries a known agency prefix, the value
    with that prefix removed. Empty/degenerate ids yield no keys."""
    norm = _normalize_id(value)
    if not norm:
        return set()
    keys = {norm}
    for prefix in _ID_PREFIXES:
        p = prefix.upper()
        if norm.startswith(p):
            stripped = norm[len(p) :].strip()
            if stripped:
                keys.add(stripped)
    return keys


# A USGS groundwater site number is 15 digits derived from the well's DMS
# coordinates: 6-digit latitude (DDMMSS) + 7-digit longitude (DDDMMSS) +
# 2-digit sequence. NWIS records it spaceless (e.g. "USGS-330519104134001"),
# but other agencies transcribe it with a space after the latitude
# ("330519 104134001") or bury it in a name field, so it never matches the
# canonical id. The 6-digit head then 8-9 digit tail (14-15 total) is
# distinctive enough to recognize without colliding with shorter surface-water
# gage numbers (e.g. "08313000").
_USGS_STATION_RE = re.compile(r"(?<!\d)(\d{6})[ \t]?(\d{8,9})(?!\d)")


def _usgs_station_keys(*values: Any) -> set[str]:
    """Canonical spaceless USGS station numbers found in the given values,
    tolerant of an embedded space and of being embedded in free text (e.g. a
    site name). Used so a transcribed USGS number cross-references the NWIS
    well whose id is that same number."""
    keys: set[str] = set()
    for value in values:
        if value is None:
            continue
        for m in _USGS_STATION_RE.finditer(str(value)):
            num = m.group(1) + m.group(2)
            if 14 <= len(num) <= 15:
                keys.add(num)
    return keys


def _parse_references(value: Any) -> list[str]:
    """Split a free-form alternate-id field into individual reference tokens.
    Agencies pack multiple ids into one field with assorted delimiters."""
    if value is None:
        return []
    text = str(value)
    for sep in (",", ";", "|", "/", "\n", "\t"):
        text = text.replace(sep, " ")
    return [tok for tok in (t.strip() for t in text.split(" ")) if tok]


def _name_tokens(*values: Any) -> set[str]:
    """Alphanumeric tokens (len >= 3) drawn from a well's name and id, for
    cheap fuzzy corroboration. Short tokens are dropped because they collide too
    easily to be evidence."""
    tokens: set[str] = set()
    for value in values:
        if value is None:
            continue
        cur = ""
        for ch in str(value).upper():
            if ch.isalnum():
                cur += ch
            else:
                if len(cur) >= 3:
                    tokens.add(cur)
                cur = ""
        if len(cur) >= 3:
            tokens.add(cur)
    return tokens


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters between two WGS84 points."""
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


def site_key(site: dict) -> str:
    """Stable "SOURCE:id" key for a site (matches the OGC feature id format)."""
    source = _normalize_id(site.get("source"))
    rid = str(site.get("id") if site.get("id") is not None else "").strip()
    return f"{source}:{rid}"


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------
class _UnionFind:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def add(self, x: str) -> None:
        self._parent.setdefault(x, x)

    def find(self, x: str) -> str:
        self.add(x)
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        # Path compression.
        while self._parent[x] != root:
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # Deterministic root choice (smaller key wins) so component ids are
        # independent of input order.
        if rb < ra:
            ra, rb = rb, ra
        self._parent[rb] = ra

    def components(self) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = defaultdict(list)
        for x in self._parent:
            groups[self.find(x)].append(x)
        return groups


# ---------------------------------------------------------------------------
# Spatial index
# ---------------------------------------------------------------------------
class _GridIndex:
    """Uniform lat/lon grid for near-neighbor candidate search. Cell size is
    derived from the link distance so only the 3x3 block around a point needs to
    be scanned. Avoids the O(n^2) all-pairs comparison on large site sets."""

    def __init__(self, distance_m: float) -> None:
        # Latitude degrees per meter is ~constant; use it (with a safety factor)
        # as the cell size so a cell spans at least the link distance in both
        # axes at NM latitudes.
        deg_per_m = 1.0 / 111_320.0
        self._cell = max(distance_m * deg_per_m, 1e-6)
        self._cells: dict[tuple[int, int], list[int]] = defaultdict(list)

    def _cell_of(self, lat: float, lon: float) -> tuple[int, int]:
        return (int(math.floor(lat / self._cell)), int(math.floor(lon / self._cell)))

    def add(self, idx: int, lat: float, lon: float) -> None:
        self._cells[self._cell_of(lat, lon)].append(idx)

    def neighbors(self, lat: float, lon: float) -> Iterable[int]:
        ci, cj = self._cell_of(lat, lon)
        for di in (-1, 0, 1):
            for dj in (-1, 0, 1):
                yield from self._cells.get((ci + di, cj + dj), ())


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------
def _explicit_links(sites: list[dict]) -> list[tuple[int, int, str, float]]:
    """Edges asserted by explicit id references. For each site, its usgs_site_id
    and every alternate_site_id token is matched against an index of the ids
    (and USGS ids) of all sites; a hit links the two sites. Self-links and
    within-source links contribute nothing new but are harmless (dropped).

    Returns ``(i, j, "explicit", confidence)`` tuples."""
    # Map every normalized match key -> the site indices reachable by it.
    index: dict[str, set[int]] = defaultdict(set)
    for i, s in enumerate(sites):
        for key in _match_keys(s.get("id")):
            index[key].add(i)
        for key in _match_keys(s.get("usgs_site_id")):
            index[key].add(i)
        # A USGS station number in this site's id / usgs_site_id / name is part
        # of its identity, so index its canonical form too.
        for key in _usgs_station_keys(s.get("id"), s.get("usgs_site_id"), s.get("name")):
            index[key].add(i)

    edges: list[tuple[int, int, str, float]] = []
    for i, s in enumerate(sites):
        refs: list[str] = []
        if s.get("usgs_site_id"):
            refs.append(str(s["usgs_site_id"]))
        refs.extend(_parse_references(s.get("alternate_site_id")))

        seen: set[str] = set()
        # Match keys from ordinary references plus any USGS station number
        # transcribed into usgs_site_id / alternate_site_id / name.
        ref_keys = {k for ref in refs for k in _match_keys(ref)}
        ref_keys |= _usgs_station_keys(
            s.get("usgs_site_id"), s.get("alternate_site_id"), s.get("name")
        )
        for key in ref_keys:
            if key in seen:
                continue
            seen.add(key)
            for j in index.get(key, ()):
                if j != i:
                    edges.append((i, j, "explicit", _CONF_EXPLICIT))
    return edges


def _spatial_confidence(
    distance_m: float,
    max_link_distance_m: float,
    si: dict,
    sj: dict,
    depth_tolerance_ft: float,
    elevation_tolerance_ft: float,
) -> Optional[float]:
    """Confidence (0..1) that two nearby cross-agency wells are the same physical
    well, or ``None`` if an attribute disagrees beyond tolerance (reject).

    Proximity alone is weak evidence because coordinates are approximate; each
    independent attribute that agrees adds confidence. Depth or elevation present
    on *both* sites and out of tolerance is disqualifying."""
    conf = _CONF_SPATIAL_BASE
    if distance_m <= max_link_distance_m / 3.0:
        conf += _CONF_BONUS_CLOSE

    depth1, depth2 = _num(si.get("well_depth")), _num(sj.get("well_depth"))
    if depth1 is not None and depth2 is not None:
        if abs(depth1 - depth2) > depth_tolerance_ft:
            return None  # same spot, clearly different construction -> reject
        conf += _CONF_BONUS_DEPTH

    elev1, elev2 = _num(si.get("elevation")), _num(sj.get("elevation"))
    if elev1 is not None and elev2 is not None:
        if abs(elev1 - elev2) > elevation_tolerance_ft:
            return None
        conf += _CONF_BONUS_ELEVATION

    tokens_i = _name_tokens(si.get("name"), si.get("id"))
    tokens_j = _name_tokens(sj.get("name"), sj.get("id"))
    if tokens_i & tokens_j:
        conf += _CONF_BONUS_NAME

    return min(conf, _CONF_SPATIAL_CAP)


def _spatial_links(
    sites: list[dict],
    keys: list[str],
    max_link_distance_m: float,
    depth_tolerance_ft: float,
    elevation_tolerance_ft: float,
) -> list[tuple[int, int, str, float]]:
    """Candidate edges from spatial proximity between *different* sources, scored
    by attribute agreement (see :func:`_spatial_confidence`). Same-source pairs
    are never linked spatially (an agency's own two wells are distinct sites, not
    duplicates). Returns ``(i, j, "spatial", confidence)`` tuples.

    Spatial links are restricted to **mutual nearest neighbors within each agency
    pair**: well *i* links to well *j* of agency *B* only if *j* is *i*'s best
    (highest-confidence, then nearest) *B* candidate **and** *i* is *j*'s best
    candidate in *i*'s agency. Proximity is not transitive, so without this a
    dense monitoring area collapses into one giant cluster as wells chain
    neighbor-to-neighbor; mutual best-match caps each well at one link per other
    agency and prevents a well from absorbing a whole neighborhood."""
    located: list[int] = []
    grid = _GridIndex(max_link_distance_m)
    src_of: dict[int, str] = {}
    for i, s in enumerate(sites):
        lat, lon = _num(s.get("latitude")), _num(s.get("longitude"))
        if lat is None or lon is None:
            continue
        grid.add(i, lat, lon)
        located.append(i)
        src_of[i] = _normalize_id(s.get("source"))

    # best[i][other_source] = (j, confidence, distance) — i's top candidate in
    # each other agency.
    best: dict[int, dict[str, tuple[int, float, float]]] = defaultdict(dict)
    for i in located:
        si = sites[i]
        lat1, lon1 = _num(si["latitude"]), _num(si["longitude"])
        src1 = src_of[i]
        for j in grid.neighbors(lat1, lon1):
            if j == i or src_of.get(j) == src1:
                continue
            sj = sites[j]
            lat2, lon2 = _num(sj["latitude"]), _num(sj["longitude"])
            dist = haversine_m(lat1, lon1, lat2, lon2)
            if dist > max_link_distance_m:
                continue
            conf = _spatial_confidence(
                dist,
                max_link_distance_m,
                si,
                sj,
                depth_tolerance_ft,
                elevation_tolerance_ft,
            )
            if conf is None:
                continue
            srcj = src_of[j]
            cur = best[i].get(srcj)
            # Higher confidence wins; ties broken by distance, then site key for
            # determinism (independent of neighbor iteration order).
            cand = (j, conf, dist)
            if cur is None or _better_candidate(cand, cur, keys):
                best[i][srcj] = cand

    edges: list[tuple[int, int, str, float]] = []
    seen: set[tuple[int, int]] = set()
    for i in located:
        src1 = src_of[i]
        for srcj, (j, conf, _dist) in best[i].items():
            # Mutual: j's best candidate in i's agency must be i.
            back = best.get(j, {}).get(src1)
            if back is None or back[0] != i:
                continue
            pair = (i, j) if i < j else (j, i)
            if pair in seen:
                continue
            seen.add(pair)
            edges.append((pair[0], pair[1], "spatial", conf))
    return edges


def _better_candidate(cand: tuple, cur: tuple, keys: list[str]) -> bool:
    """True if spatial candidate *cand* (j, conf, dist) should replace *cur*.
    Higher confidence, then shorter distance, then smaller site key — fully
    deterministic regardless of iteration order."""
    j_c, conf_c, dist_c = cand
    j_p, conf_p, dist_p = cur
    if conf_c != conf_p:
        return conf_c > conf_p
    if dist_c != dist_p:
        return dist_c < dist_p
    return keys[j_c] < keys[j_p]


def _component_id(member_keys: list[str]) -> str:
    """Stable short id for a component from its sorted member keys."""
    joined = "|".join(sorted(member_keys))
    return "wc_" + hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]


def correlate_wells(
    sites: Iterable[dict],
    *,
    max_link_distance_m: float = DEFAULT_MAX_LINK_DISTANCE_M,
    depth_tolerance_ft: float = DEFAULT_DEPTH_TOLERANCE_FT,
    elevation_tolerance_ft: float = DEFAULT_ELEVATION_TOLERANCE_FT,
    pod_link_distance_m: float = DEFAULT_POD_LINK_DISTANCE_M,
    pod_source: str = DEFAULT_POD_SOURCE,
) -> list[dict]:
    """Correlate wells across agencies and link each to OSE POD(s).

    Parameters
    ----------
    sites:
        Site dicts with at least ``source`` and ``id``; optionally ``name``,
        ``latitude``, ``longitude``, ``elevation``, ``well_depth``,
        ``usgs_site_id`` and ``alternate_site_id``.
    max_link_distance_m:
        Max great-circle distance for a cross-agency spatial link.
    depth_tolerance_ft:
        Max well-depth difference for depth to corroborate a spatial link (and
        beyond which a spatial candidate is rejected).
    elevation_tolerance_ft:
        Max surface-elevation difference for elevation to corroborate a spatial
        link (and beyond which a spatial candidate is rejected).
    pod_link_distance_m:
        Max distance for associating an OSE POD with a component spatially.
    pod_source:
        The ``source`` value identifying OSE POD sites (default ``"NMOSEPOD"``).

    Returns
    -------
    list[dict]
        One crosswalk record per input well. Duplicate ``(source, id)`` inputs
        are collapsed to the first occurrence. Records are sorted by well key
        for stable output. Each record has:

        ``source``, ``id``, ``name``, ``latitude``, ``longitude``,
        ``cluster_id``, ``cluster_size``, ``n_agencies``,
        ``linked_site_ids`` (list of "SOURCE:id" for other wells in the
        cluster), ``linked_by_agency`` (dict agency -> [ids]),
        ``ose_pod_ids`` (list), ``ose_pod_link_method``
        (``explicit`` / ``spatial`` / ``none``), ``match_method``
        (``explicit`` / ``spatial`` / ``mixed`` / ``unmatched``),
        ``match_confidence`` (0..1), ``is_ose_pod`` (bool).
    """
    # De-duplicate on (source, id), keeping the first occurrence, so a site that
    # appears twice does not inflate a cluster or the output.
    unique: list[dict] = []
    seen_keys: set[str] = set()
    for s in sites:
        k = site_key(s)
        if k in seen_keys:
            continue
        seen_keys.add(k)
        unique.append(s)

    sites = unique
    keys = [site_key(s) for s in sites]
    pod_norm = _normalize_id(pod_source)

    uf = _UnionFind()
    for k in keys:
        uf.add(k)

    # Track, per unordered site-index pair, the strongest evidence linking them
    # as (method, confidence). explicit outranks spatial; among same method the
    # higher confidence wins.
    edge_evidence: dict[tuple[int, int], tuple[str, float]] = {}
    rank = {"explicit": 2, "spatial": 1}

    def _record_edge(i: int, j: int, method: str, conf: float) -> None:
        pair = (i, j) if i < j else (j, i)
        prev = edge_evidence.get(pair)
        if (
            prev is None
            or rank[method] > rank[prev[0]]
            or (rank[method] == rank[prev[0]] and conf > prev[1])
        ):
            edge_evidence[pair] = (method, conf)

    for i, j, method, conf in _explicit_links(sites):
        _record_edge(i, j, method, conf)
    for i, j, method, conf in _spatial_links(
        sites,
        keys,
        max_link_distance_m,
        depth_tolerance_ft,
        elevation_tolerance_ft,
    ):
        _record_edge(i, j, method, conf)

    for i, j in edge_evidence:
        uf.union(keys[i], keys[j])

    # Component membership as site indices.
    key_to_index = {k: i for i, k in enumerate(keys)}
    comp_members: dict[str, list[int]] = defaultdict(list)
    for k in keys:
        comp_members[uf.find(k)].append(key_to_index[k])

    # Per-component: which methods appear among its internal edges and the best
    # spatial-edge confidence (spatial confidence is graduated by attribute
    # agreement, so the cluster reports its strongest spatial link).
    comp_methods: dict[str, set[str]] = defaultdict(set)
    comp_best_spatial: dict[str, float] = defaultdict(float)
    for (i, j), (method, conf) in edge_evidence.items():
        root = uf.find(keys[i])
        comp_methods[root].add(method)
        if method == "spatial":
            comp_best_spatial[root] = max(comp_best_spatial[root], conf)

    results: list[dict] = []
    for root, member_idxs in comp_members.items():
        member_keys = [keys[i] for i in member_idxs]
        cluster_id = _component_id(member_keys)
        methods = comp_methods.get(root, set())

        # OSE PODs in this component (explicit membership).
        pod_ids_explicit = [
            str(sites[i].get("id"))
            for i in member_idxs
            if _normalize_id(sites[i].get("source")) == pod_norm
        ]

        agencies = {_normalize_id(sites[i].get("source")) for i in member_idxs}

        if "explicit" in methods and "spatial" in methods:
            cluster_method = "mixed"
            confidence = _CONF_EXPLICIT
        elif "explicit" in methods:
            cluster_method = "explicit"
            confidence = _CONF_EXPLICIT
        elif "spatial" in methods:
            cluster_method = "spatial"
            confidence = round(comp_best_spatial[root], 3)
        else:
            cluster_method = "unmatched"
            confidence = 0.0

        for i in member_idxs:
            s = sites[i]
            k = keys[i]
            others = [ok for ok in member_keys if ok != k]
            by_agency: dict[str, list[str]] = defaultdict(list)
            for oi in member_idxs:
                if oi == i:
                    continue
                by_agency[_normalize_id(sites[oi].get("source"))].append(
                    str(sites[oi].get("id"))
                )

            is_pod = _normalize_id(s.get("source")) == pod_norm
            # POD linkage from this well's perspective.
            pod_ids = list(pod_ids_explicit)
            pod_method = (
                "explicit" if pod_ids and not (is_pod and len(pod_ids) == 1) else "none"
            )
            if is_pod:
                # A POD well is trivially "its own" POD; report others in cluster.
                pod_ids = [p for p in pod_ids_explicit if p != str(s.get("id"))]
                pod_method = "explicit" if pod_ids else "none"

            results.append(
                {
                    "source": s.get("source"),
                    "id": s.get("id"),
                    "name": s.get("name"),
                    "latitude": _num(s.get("latitude")),
                    "longitude": _num(s.get("longitude")),
                    "elevation": _num(s.get("elevation")),
                    "well_depth": _num(s.get("well_depth")),
                    "cluster_id": cluster_id,
                    "cluster_size": len(member_idxs),
                    "n_agencies": len(agencies),
                    "linked_site_ids": sorted(others),
                    "linked_by_agency": {
                        a: sorted(v) for a, v in sorted(by_agency.items())
                    },
                    "ose_pod_ids": sorted(set(pod_ids)),
                    "ose_pod_link_method": pod_method,
                    "match_method": (
                        cluster_method if len(member_idxs) > 1 else "unmatched"
                    ),
                    "match_confidence": confidence if len(member_idxs) > 1 else 0.0,
                    "is_ose_pod": is_pod,
                }
            )

    # Second pass: spatially associate PODs with components that contain no POD,
    # so wells with no explicit POD reference still get a candidate POD. This is
    # done after clustering so a POD links to the whole component, not one member.
    _link_pods_spatially(sites, keys, uf, results, pod_norm, pod_link_distance_m)

    results.sort(key=lambda r: f"{_normalize_id(r['source'])}:{r['id']}")
    return results


def _link_pods_spatially(
    sites: list[dict],
    keys: list[str],
    uf: _UnionFind,
    results: list[dict],
    pod_norm: str,
    pod_link_distance_m: float,
) -> None:
    """For components that contain no explicit OSE POD, attach the nearest POD(s)
    within ``pod_link_distance_m`` of any member as *candidate* POD links. POD
    coordinates are approximate, so this is advisory (method ``spatial``)."""
    pod_idxs = [
        i for i, s in enumerate(sites) if _normalize_id(s.get("source")) == pod_norm
    ]
    if not pod_idxs:
        return

    grid = _GridIndex(pod_link_distance_m)
    for i in pod_idxs:
        lat, lon = _num(sites[i].get("latitude")), _num(sites[i].get("longitude"))
        if lat is None or lon is None:
            continue
        grid.add(i, lat, lon)

    # Which components already have an explicit POD?
    comp_has_pod: dict[str, bool] = defaultdict(bool)
    for i in pod_idxs:
        comp_has_pod[uf.find(keys[i])] = True

    # Nearest POD per component (that lacks one), scanning each member's cell.
    comp_best: dict[str, tuple[float, str]] = {}
    result_by_key = {f"{_normalize_id(r['source'])}:{r['id']}": r for r in results}
    for idx, k in enumerate(keys):
        root = uf.find(k)
        if comp_has_pod.get(root):
            continue
        lat, lon = _num(sites[idx].get("latitude")), _num(sites[idx].get("longitude"))
        if lat is None or lon is None:
            continue
        for pj in grid.neighbors(lat, lon):
            plat = _num(sites[pj].get("latitude"))
            plon = _num(sites[pj].get("longitude"))
            d = haversine_m(lat, lon, plat, plon)
            if d > pod_link_distance_m:
                continue
            pod_id = str(sites[pj].get("id"))
            best = comp_best.get(root)
            if best is None or d < best[0] or (d == best[0] and pod_id < best[1]):
                comp_best[root] = (d, pod_id)

    for idx, k in enumerate(keys):
        root = uf.find(k)
        best = comp_best.get(root)
        if best is None:
            continue
        r = result_by_key.get(k)
        if r is None:
            continue
        r["ose_pod_ids"] = [best[1]]
        r["ose_pod_link_method"] = "spatial"


# ============= EOF =============================================
