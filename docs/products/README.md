# DIE products: data dictionary and methods

The Data Integration Engine publishes each product as an OGC API - Features
collection. Each collection links to the section below for its output type
(`rel: describedby`). Sections are headed by the output type id, so the link
anchor is `#<output_type>`.

Don't rename this file or its section headings: the collection links depend on
them. `tests/test_product_docs.py` checks that every linked section exists.

## Common fields

Every layer is GeoJSON or GeoParquet with WGS 84 coordinates (CRS84). Besides the
fields listed for each output type:

| Field | Description |
|---|---|
| `feature_id` | Unique feature id (GeoParquet only). `source:id`, with `:2`, `:3`, … appended to repeats. |
| `bbox` | Bounding-box covering column (GeoParquet only), used for spatial filtering. |

## Sources

Each feature's `source` field names the agency its data came from. A collection's
schema page (`/collections/<id>/schema`) lists the `source` values in that layer
under **Values**.

| Source value | Abbreviation | Provider |
|---|---|---|
| `NMBGMR` | NMBGMR | New Mexico Bureau of Geology and Mineral Resources, Aquifer Mapping Program |
| `USGS-NWIS` | USGS NWIS | U.S. Geological Survey National Water Information System |
| `WQP` | WQP | Water Quality Portal |
| `ST/DWB` | NMED DWB | New Mexico Environment Department Drinking Water Bureau |
| `BOR-RISE` | USBR | U.S. Bureau of Reclamation (RISE) |
| `ST2/NMOSE-Roswell` | NMOSE Roswell | New Mexico Office of the State Engineer, Roswell District |
| `NMOSEPOD` | NM OSE | New Mexico Office of the State Engineer, Points of Diversion |
| `ISCSevenRivers` | NM ISC | New Mexico Interstate Stream Commission, Seven Rivers |
| `ST2/BernCo` | BernCo | Bernalillo County |
| `ST2/CABQ` | CABQ | City of Albuquerque |
| `ST2/EBID` | EBID | Elephant Butte Irrigation District |
| `ST2/PVACD` | PVACD | Pecos Valley Artesian Conservancy District |

Water Quality Portal values may carry the data provider, as `WQP/<provider>`
(e.g. `WQP/NWIS`, `WQP/STORET`). Collection abstracts list sources by
abbreviation. This table mirrors `orchestration/config/sources.yaml`;
`tests/test_product_docs.py` checks they match.

## ogc_waterlevel_trend

**Products:** `nm_waterlevel_trends` (full record, the overall trend),
`nm_waterlevel_trends_5yr`, `nm_waterlevel_trends_10yr`, `nm_waterlevel_trends_20yr`

One point feature per well that has depth-to-water measurements, labeled with
the direction and rate of change of its water level.

### Method

> Depth-to-water trend per well. Observations are reduced to one point per calendar month (UTC), keeping the monthly MEDIAN depth-to-water. Monotonic trend is tested with the non-parametric Mann-Kendall test (pymannkendall.original_test, alpha=0.05) on the monthly series ordered by time; the rate is the Theil-Sen slope vs time. A well is classified only when it has at least 10 monthly points, or at least 4 monthly points spanning at least 2 years; otherwise 'not enough data'. A significant increasing slope is 'increasing' (water level getting DEEPER, i.e. a declining water table), a significant decreasing slope is 'decreasing' (water level getting SHALLOWER), else 'stable'.

In plain terms: the trend is in **depth to water**, so `increasing` means the
water level is falling and `decreasing` means it is rising. A `stable` well has
no statistically significant trend at the 95% level. That doesn't prove the
level is flat, especially for short records.

Reducing to monthly medians keeps dense logger data (many readings a day) from
counting near-duplicate readings as independent evidence. It does **not**
remove seasonal patterns: the test assumes there are none, so a strong seasonal
swing can still affect the result, most of all in the 5-year layer (Helsel et
al. 2020, §12.4).

### Period

`nm_waterlevel_trends` covers each well's **full record** (`trend_period` =
`full record`); its `window_*` fields are empty.

The `_5yr`, `_10yr` and `_20yr` layers cover a **fixed window** of whole
calendar months before the month of the run, the same for every well. For
example, a run in November 2026 gives the 5-year layer a window from 2021-11-01
to 2026-10-31; the partial run month is left out. `window_start` and
`window_end` give the exact dates, and the collection's `trend_method` states
them too. Only measurements inside the window are used, and the minimum-data
rule (at least 10 monthly values, or at least 4 spanning 2 or more years)
applies to the monthly values inside the window. A short window can catch one
side of a wet/dry cycle, so read the 5-year layer alongside the 10- and 20-year
ones.

Wells that fail the rule are `not enough data`; `insufficient_reason` says why.

The Mann-Kendall test (Mann 1945; Kendall 1975) checks for a monotonic trend
without assuming the data are normally distributed or the change is linear. The
Theil-Sen slope (Theil 1950; Sen 1968) is the median of the slopes between all
pairs of points, so a few outliers don't skew it. See [References](#references).

### Fields

| Field | Description | Units | Values |
|---|---|---|---|
| `source` | Agency the well's data came from (see [Sources](#sources)) | | |
| `id` | Well id at the source agency | | |
| `name` | Well name at the source agency | | may be empty |
| `parameter_name` | Analyte name; always empty for water levels | | null |
| `well_depth` | Total well depth, when the source reports it | ft | null if unknown |
| `well_depth_units` | Units of `well_depth` | | `ft` |
| `trend_period` | Period the trend covers | | `full record`, `5 years`, `10 years`, `20 years` |
| `window_years` | Length of the fixed window | years | 5, 10, 20; null for the full record |
| `window_start` | Start of the fixed window, the first of a month (UTC, ISO 8601) | | null for the full record |
| `window_end` | End of the fixed window, the last second of the month before the run (UTC, ISO 8601) | | null for the full record |
| `record_count` | Number of monthly values used in the test | | ≥ 0 |
| `observation_count` | Number of raw measurements (in the window, for windowed layers) before reducing to monthly values | | ≥ 0 |
| `first_observation_datetime` | Month of the first monthly value (UTC, ISO 8601, first of the month) | | |
| `last_observation_datetime` | Month of the last monthly value (UTC, ISO 8601, first of the month) | | |
| `span_years` | Time from first to last monthly value | years | ≥ 0 |
| `slope_per_year` | Theil-Sen slope of depth to water vs time; positive = getting deeper | ft/year | null when `not enough data` |
| `slope_units` | Units of `slope_per_year` | | `ft/year` |
| `trend_category` | Mann-Kendall result | | `increasing`, `decreasing`, `stable`, `not enough data` |
| `insufficient_reason` | Why the well is `not enough data` | | text; null when classified |
| `mk_p_value` | Mann-Kendall p-value; significant when < 0.05 | | 0–1, null when `not enough data` |
| `mk_tau` | Kendall's tau, the strength and sign of the trend | | −1 to 1, null when `not enough data` |
| `source_datastream_link` | Link to the raw source datastream, when the source has one (SensorThings sources) | | URL; field absent otherwise |

## References

- Mann, H. B. (1945). Nonparametric tests against trend. Econometrica, 13(3), 245–259. https://doi.org/10.2307/1907187
- Kendall, M. G. (1975). Rank Correlation Methods. Charles Griffin, London.
- Theil, H. (1950). A rank-invariant method of linear and polynomial regression analysis, I, II, III. Proceedings of the Koninklijke Nederlandse Akademie van Wetenschappen, 53, 386–392, 521–525, 1397–1412.
- Sen, P. K. (1968). Estimates of the regression coefficient based on Kendall’s tau. Journal of the American Statistical Association, 63(324), 1379–1389. https://doi.org/10.1080/01621459.1968.10480934
- Helsel, D. R., Hirsch, R. M., Ryberg, K. R., Archfield, S. A., & Gilroy, E. J. (2020). Statistical Methods in Water Resources. U.S. Geological Survey Techniques and Methods, book 4, chap. A3. https://doi.org/10.3133/tm4A3

Implementations used:

- Mann-Kendall: [pyMannKendall](https://github.com/mmhs013/pyMannKendall) `original_test`. Hussain, M. M., & Mahmud, I. (2019). pyMannKendall: a python package for non parametric Mann Kendall family of trend tests. *Journal of Open Source Software*, 4(39), 1556. https://doi.org/10.21105/joss.01556
- Theil-Sen: [`scipy.stats.theilslopes`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.theilslopes.html)
