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

Water-level products combine measurements from:

| Source value | Provider |
|---|---|
| `NMBGMR` | New Mexico Bureau of Geology and Mineral Resources, Aquifer Mapping Program |
| `USGS-NWIS` | USGS National Water Information System |
| `WQP/<provider>` | Water Quality Portal, tagged with the data provider (e.g. `WQP/NWIS`) |
| `ST2/NMOSE-Roswell` | NM Office of the State Engineer, Roswell district |
| `ISCSevenRivers` | NM Interstate Stream Commission, Seven Rivers |
| `ST2/BernCo` | Bernalillo County |
| `ST2/CABQ` | City of Albuquerque |
| `ST2/EBID` | Elephant Butte Irrigation District |
| `ST2/PVACD` | Pecos Valley Artesian Conservancy District |

## ogc_waterlevel_trend

**Products:** `nm_waterlevel_trends`

One point feature per well that has depth-to-water measurements, labeled with
the direction and rate of change of its water level.

### Method

> Depth-to-water trend per well. Observations are downsampled to one point per calendar day, keeping the daily MINIMUM depth-to-water (shallowest reading). Monotonic trend is tested with the non-parametric Mann-Kendall test (pymannkendall.original_test, alpha=0.05) on the daily series ordered by time; the rate is the Theil-Sen slope vs time. A well is classified only when it has at least 10 daily points, or at least 4 daily points spanning at least 2 years; otherwise 'not enough data'. A significant increasing slope is 'increasing' (water level getting DEEPER, i.e. a declining water table), a significant decreasing slope is 'decreasing' (water level getting SHALLOWER), else 'stable'.

In plain terms: the trend is in **depth to water**, so `increasing` means the
water level is falling and `decreasing` means it is rising. A `stable` well has
no statistically significant trend at the 95% level. That doesn't prove the
level is flat, especially for short records.

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
| `record_count` | Number of daily values used in the test | | ≥ 0 |
| `observation_count` | Number of raw measurements before reducing to daily values | | ≥ 0 |
| `first_observation_datetime` | Date of the first daily value (UTC, ISO 8601) | | |
| `last_observation_datetime` | Date of the last daily value (UTC, ISO 8601) | | |
| `span_years` | Time from first to last daily value | years | ≥ 0 |
| `slope_per_year` | Theil-Sen slope of depth to water vs time; positive = getting deeper | ft/year | null when `not enough data` |
| `slope_units` | Units of `slope_per_year` | | `ft/year` |
| `trend_category` | Mann-Kendall result | | `increasing`, `decreasing`, `stable`, `not enough data` |
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
