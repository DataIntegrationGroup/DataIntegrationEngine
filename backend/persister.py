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


class BasePersister:
    """In-memory accumulator for a unification pass.

    ``_site_wrapper`` fills the three lists; the Dagster source assets read them
    back (summary records, and index-aligned sites/timeseries) and serialize via
    the GeoPandas path. The former CSV/GeoJSON byte assembly + write strategies
    served the removed CLI/worker output path and are gone.
    """

    def __init__(self, config=None):
        self.config = config
        self.records = []      # SummaryRecord (summary mode)
        self.sites = []        # SiteRecord (timeseries mode)
        self.timeseries = []   # list[list[ParameterRecord]], aligned with sites


# ============= EOF =============================================
