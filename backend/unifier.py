# ===============================================================================
# Copyright 2024 Jake Ross
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
from concurrent.futures import ThreadPoolExecutor, as_completed

from backend.persisters.factory import make_persister
from backend.exceptions import USGSRateLimitError, PartialOrNoDataError


def _site_wrapper(site_source, parameter_source, persister, config, raise_errors=False):

    try:
        # snapshot lengths to roll back to on a rate-limit / partial-data abort,
        # so a source never contributes partial records
        initial_sites_len = len(persister.sites)
        initial_timeseries_len = len(persister.timeseries)
        initial_records_len = len(persister.records)

        incomplete_sites_record_msg = f"Failed to retrieve complete site records for {site_source}. No records will be saved for this source."
        incomplete_parameter_record_msg = f"Failed to retrieve complete parameter records for {site_source}. No records will be saved for this source."

        use_summarize = config.output_summary
        site_limit = config.site_limit

        try:
            sites = site_source.read()
        except (USGSRateLimitError, PartialOrNoDataError):
            config.warn(incomplete_sites_record_msg)
            sites = []

        if not sites:
            return

        sites_with_records_count = 0
        start_ind = 0
        end_ind = 0
        first_flag = True

        if config.sites_only:
            persister.sites.extend(sites)
        else:
            # Build the chunk list up front with each chunk's advisory log
            # indices (start_ind/end_ind feed only log messages downstream).
            chunk_specs = []
            for site_records in site_source.chunks(sites):
                if type(site_records) == list:
                    n = len(site_records)
                    if first_flag:
                        first_flag = False
                    else:
                        start_ind = end_ind + 1
                    end_ind += n
                chunk_specs.append((site_records, start_ind, end_ind))

            # Fetch chunks concurrently (network-bound), but keep results in
            # chunk order so output and site_limit stay deterministic. The
            # persister is mutated only below, on this thread.
            def _fetch(spec):
                records, s_ind, e_ind = spec
                return parameter_source.read(records, use_summarize, s_ind, e_ind)

            workers = max(int(getattr(config, "fetch_workers", 1) or 1), 1)
            workers = min(workers, len(chunk_specs)) if chunk_specs else 1

            results_by_chunk = [None] * len(chunk_specs)
            aborted = False
            if workers > 1:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {executor.submit(_fetch, spec): i for i, spec in enumerate(chunk_specs)}
                    for future in as_completed(futures):
                        try:
                            results_by_chunk[futures[future]] = future.result()
                        except (USGSRateLimitError, PartialOrNoDataError):
                            aborted = True
                            break
            else:
                for i, spec in enumerate(chunk_specs):
                    try:
                        results_by_chunk[i] = _fetch(spec)
                    except (USGSRateLimitError, PartialOrNoDataError):
                        aborted = True
                        break

            if aborted:
                # remove partial records to prevent incomplete data from being saved
                persister.sites = persister.sites[:initial_sites_len]
                persister.timeseries = persister.timeseries[:initial_timeseries_len]
                persister.records = persister.records[:initial_records_len]
                config.warn(incomplete_parameter_record_msg)
            else:
                for results in results_by_chunk:
                    if use_summarize:
                        if results:
                            persister.records.extend(results)
                            sites_with_records_count += len(results)
                        else:
                            continue
                    else:
                        # no records are returned if there is no site record for parameter
                        # or if the record isn't clean (doesn't have the correct fields)
                        # don't count these sites to apply to site_limit
                        if results is None or len(results) == 0:
                            continue
                        else:
                            sites_with_records_count += len(results)

                        for site, records in results:
                            persister.timeseries.append(records)
                            persister.sites.append(site)

                    if site_limit:
                        if sites_with_records_count >= site_limit:
                            # remove any extra sites that were gathered. removes 0 if site_limit is not exceeded
                            num_sites_to_remove = sites_with_records_count - site_limit

                            # if sites_with_records_count == sit_limit then num_sites_to_remove = 0
                            # and calling list[:0] will retur an empty list, so subtract
                            # num_sites_to_remove from the length of the list
                            # to remove the last num_sites_to_remove sites
                            if use_summarize:
                                persister.records = persister.records[
                                    : len(persister.records) - num_sites_to_remove
                                ]
                            else:
                                persister.timeseries = persister.timeseries[
                                    : len(persister.timeseries) - num_sites_to_remove
                                ]
                                persister.sites = persister.sites[
                                    : len(persister.sites) - num_sites_to_remove
                                ]
                            break

    except Exception:
        import traceback

        config.warn(traceback.format_exc())
        config.warn(f"Failed to unify {site_source}")
        if raise_errors:
            raise


def unify_source(config, source_key):
    """Run unification for a single source and return its persister.

    Used by the per-source Dagster assets so each source's contribution
    (records/sites/timeseries) can be materialized and observed independently.
    Unexpected errors propagate (raise_errors=True) so the caller can mark the
    source as failed; rate-limit / partial-data conditions are still handled
    gracefully inside _site_wrapper.
    """
    config.validate()

    persister = make_persister(config)
    config._persister = persister

    pair = config.source_pair(source_key)
    if pair is None:
        config.warn(
            f"Source {source_key!r} does not provide parameter {config.parameter!r}"
        )
        return persister

    site_source, parameter_source = pair
    _site_wrapper(site_source, parameter_source, persister, config, raise_errors=True)
    return persister


def unify_source_both(config, source_key):
    """Unify a single source for BOTH summary and timeseries outputs while
    fetching the source only once.

    Each connector's ``get_records`` is mode-agnostic — summary and timeseries
    both pull the same raw observations and differ only in how they are
    transformed (see backend/source.py). Running ``unify_source`` twice would
    therefore hit the API twice for identical data. This driver instead enables
    the source's shared-fetch cache and runs the two transform passes over one
    fetch, so a source needed by both a summary and a timeseries product is
    pulled once.

    Output is identical to calling ``unify_source`` twice (once per mode); only
    the underlying fetch is shared. Returns ``(summary_persister,
    timeseries_persister)``. Used by the orchestration shared source asset.
    """
    config.validate()

    pair = config.source_pair(source_key)
    if pair is None:
        config.warn(
            f"Source {source_key!r} does not provide parameter {config.parameter!r}"
        )
        return make_persister(config), make_persister(config)

    site_source, parameter_source = pair
    # Share the site list and observation fetch across the two passes. The
    # passes issue identical requests (same parameter/scope/dates), so the
    # second reuses the first's cached fetch instead of re-querying.
    site_source._fetch_cache_enabled = True
    parameter_source._fetch_cache_enabled = True

    # Timeseries pass first so its fetch primes the cache; the summary pass then
    # transforms the same cached observations. output_summary is read live by
    # the transformer, so toggling it here switches the record klass/fields per
    # pass without rebuilding the source.
    config.output_summary = False
    timeseries_persister = make_persister(config)
    config._persister = timeseries_persister
    _site_wrapper(
        site_source, parameter_source, timeseries_persister, config, raise_errors=True
    )

    config.output_summary = True
    summary_persister = make_persister(config)
    config._persister = summary_persister
    _site_wrapper(
        site_source, parameter_source, summary_persister, config, raise_errors=True
    )

    return summary_persister, timeseries_persister


def unify_source_multi(config, source_key, parameters):
    """Unify a single source for MULTIPLE analytes with **one** fetch.

    The provider (e.g. WQP) returns every requested analyte in a single query, so
    instead of sweeping the same wells once per analyte, the source is fetched
    once and each analyte's summary + timeseries is produced by filtering that
    shared fetch. This is the fix for the per-analyte refetch redundancy (see
    orchestration/assets/products.py) — WQP's ~13 analyte assets collapse to one
    fetch.

    Returns ``{parameter: (summary_persister, timeseries_persister)}``. Analytes
    only — do not pass ``waterlevels``.

    Two regimes, chosen by whether the source's API can return several analytes
    in one query (``set_parameters`` present):

    - **WQP** — one station/result query carries every analyte, so the source is
      fetched once and each analyte's pass filters that shared fetch. Real API
      saving (~13 analyte fetches → 1).
    - **BOR / AMP / ISC / DWB** — the provider API is analyte-scoped (one query
      per analyte), so the fetch cannot collapse. Fall back to a per-analyte
      unify with a **fresh** source pair each time (these connectors cache
      analyte-specific state — BOR's catalog index, ISC's analyte ids — that a
      reused instance would carry into the next analyte). Same API calls as
      before; the payoff is one multi-analyte asset instead of many (Dagster
      graph consolidation), not fewer fetches.
    """
    config.validate()
    if not parameters:
        return {}

    # source_pair selects the analyte table from config.parameter; any analyte
    # in the group resolves the same (site, analyte) source pair.
    config.parameter = parameters[0]
    pair = config.source_pair(source_key)
    if pair is None:
        config.warn(
            f"Source {source_key!r} does not provide the requested analytes"
        )
        return {p: (make_persister(config), make_persister(config)) for p in parameters}

    site_source, parameter_source = pair
    shared_fetch = hasattr(site_source, "set_parameters") and hasattr(
        parameter_source, "set_parameters"
    )

    if not shared_fetch:
        # Analyte-scoped API: per-analyte unify with a fresh pair each time.
        result: dict = {}
        for p in parameters:
            config.parameter = p
            result[p] = unify_source_both(config, source_key)
        return result

    # Shared-fetch (WQP): fetch once for all analytes; each pass filters it to
    # its own analyte (see WQPParameterSource._extract_site_records).
    site_source.set_parameters(parameters)
    parameter_source.set_parameters(parameters)
    site_source._fetch_cache_enabled = True
    parameter_source._fetch_cache_enabled = True

    result = {}
    for p in parameters:
        config.parameter = p

        config.output_summary = False
        ts_persister = make_persister(config)
        config._persister = ts_persister
        _site_wrapper(
            site_source, parameter_source, ts_persister, config, raise_errors=True
        )

        config.output_summary = True
        summary_persister = make_persister(config)
        config._persister = summary_persister
        _site_wrapper(
            site_source, parameter_source, summary_persister, config, raise_errors=True
        )

        result[p] = (summary_persister, ts_persister)

    return result


def collect_sites(config):
    """Gather site records from **every** enabled source (sites only, no
    parameter data) and return them as a flat list of ``_payload`` dicts.

    Used by the cross-agency well-correlation product, which needs the location
    of every well across all agencies — including OSE PODs, which carry no
    parameter time series. Runs in ``sites_only`` mode over
    ``all_site_sources()`` so a source that provides no parameter (e.g. the OSE
    POD source) still contributes its sites. Errors on individual sources are
    swallowed by ``_site_wrapper`` so one dead source does not abort the sweep.
    """
    config.validate()

    config.sites_only = True
    persister = make_persister(config)
    config._persister = persister

    for site_source, _ in config.all_site_sources():
        _site_wrapper(site_source, None, persister, config)

    return [s._payload for s in persister.sites]


# ============= EOF =============================================
