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
from json import JSONDecodeError

import httpx
import shapely.wkt
from shapely import MultiPoint
from typing import Union, List, Callable, Dict

from backend.constants import (
    FEET,
    DT_MEASURED,
    PARAMETER_NAME,
    PARAMETER_UNITS,
    PARAMETER_VALUE,
    EARLIEST,
    LATEST,
)
from backend.logger import Loggable
from backend.record import (
    AnalyteRecord,
    AnalyteSummaryRecord,
    WaterLevelRecord,
    WaterLevelSummaryRecord,
    SiteRecord,
)
from backend.transformer import BaseTransformer, convert_units


def make_site_list(site_record: list[SiteRecord] | SiteRecord) -> list | str:
    """
    Returns a list of site ids, as defined by site_record

    Parameters
    ----------
    site_record: SiteRecord or list of SiteRecords

    Returns
    -------
    list
        a list of site ids
    """
    if isinstance(site_record, list):
        sites = [r.id for r in site_record]
    else:
        sites = site_record.id
    return sites


def get_terminal_record(records: list, tag: Union[str, Callable], bookend: str) -> dict:
    """
    Returns the most recent record based on the tag

    Parameters
    ----------
    records: list
        a list of records

    tag: str or callable
        the tag to use to sort the records

    bookend: str
        determines if the earliest or lastest record is retrieved

    Returns
    -------
    dict
        the most recent record for every site
    """
    if callable(tag):
        func = tag
    else:
        if "." in tag:

            def func(x):
                for t in tag.split("."):
                    x = x[t]
                return x

        else:

            def func(x):
                return x[tag]

    if bookend == EARLIEST:
        return sorted(records, key=func)[0]
    elif bookend == LATEST:
        return sorted(records, key=func)[-1]
    else:
        raise ValueError(
            f"Invalid bookend {bookend}. Must be either {EARLIEST} or {LATEST}"
        )


def get_analyte_search_param(parameter: str, mapping: dict) -> str:
    """
    Get the search parameter for a provided analyte, as defined by the mapping for a source

    Parameters
    ----------
    parameter : str
        the analyte name used in the query

    mapping : dict
        a mapping of analytes to search parameters for the source

    Returns
    -------
    str
        the search parameter for the provided analyte for a particular source
    """
    try:
        return mapping[parameter]
    except KeyError:
        raise ValueError(
            f"Invalid parameter name {parameter}. Valid parameters are {list(mapping.keys())}"
        )


class BaseSource(Loggable):
    """
    The BaseSource class is a base class for all sources, whether it be a site source or a parameter source.

    ============================================================================
    Attributes
    ============================================================================
    transformer_klass : BaseTransformer

    config : Config
        the configuration class for the source

    tag : str
    ============================================================================
    Methods With Universal Implementations (Already Implemented)
    ============================================================================
    warn
        Prints warning messages to the console in red

    log
        Prints the message to the console in yellow

    _execute_text_request
        Executes a get request to the provided url with query parameters and and returns the text response

    _execute_json_request
        Executes a get request to the provided url with query parameters and and returns the json response

    ============================================================================
    Methods Implemented in BaseSiteSource and BaseParameterSource
    ============================================================================
    read
        Returns a list of transformed records

    ============================================================================
    Methods That Need to be Implemented For Each Source
    ============================================================================
    health
        Determines if the source is healthy

    get_records
        Returns the site or parameter records from the source
    """

    transformer_klass = BaseTransformer

    def __init__(self):
        self.transformer = self.transformer_klass()
        super().__init__()

    @property
    def tag(self):
        return self.__class__.__name__.lower()

    def set_config(self, config):
        self.config = config
        self.transformer.set_config(config)

    def check(self, *args, **kw):
        return True
        # raise NotImplementedError(f"check not implemented by {self.__class__.__name__}")

    def discover(self, *args, **kw):
        return []
        # raise NotImplementedError(f"discover not implemented by {self.__class__.__name__}")

    # ==========================================================================
    # Methods Already Implemented
    # ==========================================================================

    def _execute_text_request(self, url: str, params: dict | None = None, **kw) -> str:
        """
        Executes a get request to the provided url and returns the text response.

        Parameters
        ----------
        url : str
            the url to request

        params : dict
            key-value query parameters to pass to the get request

        Returns
        -------
        str
            the text responses
        """
        if "timeout" not in kw:
            kw["timeout"] = 10

        resp = httpx.get(url, params=params, **kw)
        if resp.status_code == 200:
            return resp.text
        else:
            self.warn(f"service url {resp.url}")
            self.warn(f"service responded with status {resp.status_code}")
            self.warn(f"service responded with text {resp.text}")
            return ""

    def _execute_json_request(
        self, url: str, params: dict | None = None, tag: str | None = None, **kw
    ) -> dict | None:
        """
        Executes a get request to the provided url and returns the json response.

        Parameters
        ----------
        url : str
            the url to request

        params : dict
            key-value query parameters to pass to the get request

        tag : str
            the key to extract from the json response if required

        Returns
        -------
        dict
            the json response
        """
        resp = httpx.get(url, params=params, **kw)
        if tag is None:
            tag = "data"

        if resp.status_code == 200:
            try:
                obj = resp.json()
                if tag and isinstance(obj, dict):
                    return obj[tag]
                return obj
            except JSONDecodeError:
                self.warn(f"service responded but with no data. \n{resp.text}")
                return None
        else:
            self.warn(f"service responded with status {resp.status_code}")
            self.warn(f"service responded with text {resp.text}")
            self.warn(f"service at url:  {resp.url}")
            return None

    # ==========================================================================
    # Methods Implemented in BaseSiteSource and BaseParameterSource
    # ==========================================================================

    def read(self, *args, **kw) -> list | None:
        """
        Returns the records. Implemented in BaseSiteSource and BaseAnalyteSource
        """
        raise NotImplementedError(f"read not implemented by {self.__class__.__name__}")

    # ==========================================================================
    # Methods That Need to be Implemented For Each Source
    # ==========================================================================

    def get_records(self, *args, **kw) -> List[Dict]:
        """
        Returns records as a dictionary, where the keys are site ids and
        the values are site or parameter records.

        If site records, the values are dictionaries with the site records.

        If parameter records, the values are lists of dictionaries with parameter records.

        Called by the read method. Needs to be implemented by all subclasses.

        Parameters
        ----------
        If parameter records:
            site_record : dict
                the site record for the location whose parameter records are to be retrieved

        If site records:
            There are no parameters

        Returns
        -------
        dict
            a dictionary of site or parameter records, where the keys are site ids
            and the values are site or parameter records
        """
        raise NotImplementedError(
            f"get_records not implemented by {self.__class__.__name__}"
        )

    def health(self) -> bool:
        """
        Checks the health of the source. Implemented for each site source

        Returns
        --------
        bool
            True if the source is healthy, else False
        """
        raise NotImplementedError(f"test not implemented by {self.__class__.__name__}")


class BaseContainerSource(BaseSource):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

        # locate image
        # make container
        # container writes messages to stdout
        # this class captures the messages from stdout

    def check(self):
        # run the container with the check command
        pass

    def discover(self, *args, **kw):
        # run the container with the discover command
        pass

    def read(self, *args, **kw):
        # run the container with the read command
        pass


class BaseSiteSource(BaseSource):
    """
    The BaseSiteSource class is a base class for all site sources.
    It provides a common interface for all site sources

    Attributes
    ----------
    chunk_size : int
        the number of records to process at once

    bounding_polygon : str
        a WKT string defining the bounding polygon for the site sources


    Methods With Universal Implementations (Already Implemented)
    -------
    generate_bounding_polygon
        Generates a bounding polygon based on the site records

    intersects(wkt)
        Returns True if the bounding polygon intersects with the provided WKT string

    read(*args, **kw)
        Reads the site records and returns the transformed records, where the
        transform standardizes the records so the format is the same for all sources

    _transform_sites(records)
        Transforms the site records into the standardized format and returns
        the transformed records

    chunks(records, chunk_size=None)
        Returns a list of records split into lists of size chunk_size. If
        chunk_size less than 1 then the records are not split


    Methods That Need to be Implemented For Each Source
    -------
    get_records
        Returns a dictionary of site records, where the keys are the site ids
        and the values are the site records

    health
        Checks the health of the source
    """

    chunk_size = 1
    bounding_polygon = None

    @property
    def tag(self):
        return self.__class__.__name__.lower().replace("sitesource", "")

    def generate_bounding_polygon(self):
        """
        Generates a bounding MultiPolygon base on the longitude and latitude
        of each site record
        """
        records = self.read_sites()
        print(records[0].latitude)
        mpt = MultiPoint([(r.longitude, r.latitude) for r in records])
        print(mpt.convex_hull.buffer(1 / 60.0).wkt)
        # print(mpt.convex_hull.wkt)

    def intersects(self, wkt: str) -> bool:
        """
        Determines if the bounding polygon intersects with the provided WKT string

        Parameters
        ----------
        wkt : str
            a WKT string

        Returns
        -------
        bool
            True if the bounding polygon intersects with the provided WKT string
            True if there is no bounding polygon
        """
        if self.bounding_polygon:
            wkt = shapely.wkt.loads(wkt)
            return self.bounding_polygon.intersects(wkt)

        return True

    def read(self, *args, **kw) -> List[SiteRecord] | None:
        """
        Returns a list of transformed site records.
        Calls self.get_records, which needs to be implemented for each source

        Returns
        -------
        list[SiteRecord]
            a list of transformed site records
        """
        self.log("Gathering site records")
        records = self.get_records()
        if records:
            self.log(f"total records={len(records)}")
            return self._transform_sites(records)
        else:
            self.warn("No site records returned")
            return None

    def _transform_sites(self, records: list) -> List[SiteRecord]:
        """
        Transforms site records into the standardized format.

        Parameters
        ----------
        records : list
            a list of site records

        Returns
        -------
        list[SiteRecord]
            a list of transformed site records as SiteRecords
        """
        transformed_records = []
        for record in records:
            record = self.transformer.do_transform(record)
            if record:
                record.chunk_size = self.chunk_size
                transformed_records.append(record)

        self.log(f"processed nrecords={len(transformed_records)}")
        return transformed_records

    def chunks(self, records: list, chunk_size: int | None = None) -> list:
        """
        Returns a list of records split into lists of size chunk_size. If
        chunk_size less than 1 then the records are not split

        Parameters
        ----------
        records : list
            a list of records

        chunk_size : int
            the size of the chunks

        Returns
        -------
        list
            a list of records split into lists of size chunk_size. If chunk_size
            less than 1 then the records are not split
        """
        if chunk_size is None:
            chunk_size = self.chunk_size

        if chunk_size > 1:
            return [
                records[i : i + chunk_size] for i in range(0, len(records), chunk_size)
            ]
        else:
            return records


class BaseParameterSource(BaseSource):
    """
    The BaseParameterSource class is a base class for all parameter sources,
    whether it be an analyte source or a water level source.

    ============================================================================
    Methods With Universal Implementations (Already Implemented)
    ============================================================================

    _extract_earliest_record
        Returns the earliest record for a particular site. Requires _extract_terminal_record
        to be implemented for each source

    _extract_latest_record
        Returns the most recent record for a particular site. Requires _extract_terminal_record
        to be implemented for each source

    read
        Reads the parameter records and returns the transformed records, where the
        transform standardizes the records so the format is the same for all sources


    ============================================================================
    Methods Implemented in BaseAnalyteSource and BaseWaterLevelSource
    ============================================================================

    _validate_record
        Validates the record to ensure it has the required fields

    _get_output_units
        Returns the output units for the source

    ============================================================================
    Methods That Need to be Implemented For Each Source
    ============================================================================

    get_records
        Returns a dictionary of parameter records where the keys are the site ids
        and the values are a list of the parameter records

    _extract_site_records
        Returns all records for a single site as a list of records

    _extract_terminal_record
        Returns the terminal record for a particular site. This is only used for
        summary, not time series, outputs.

    _clean_records (optional)
        Returns cleaned records if this function is defined for each source.
        Otherwise returns the records as-is

    _extract_source_parameter_units
        Returns the units of the parameter records as a list, in the same order as the records themselves

    _extract_parameter_record
        Returns a parameter record with standardized fields added.

        - backend.constants.PARAMETER
        - backend.constants.PARAMETER_VALUE
        - backend.constants.PARAMETER_UNITS

    _extract_source_parameter_results
        Returns the parameter results as a list from the records, in the same order as the records themselves
    """

    name = ""

    # ==========================================================================
    # Methods Already Implemented
    # ==========================================================================

    def _extract_earliest_record(self, records: list) -> dict:
        """
        Returns the earliest record for a particular site

        Parameters
        ----------
        records : list
            a list of records

        Returns
        -------
        dict
            the earliest record
        """
        return self._extract_terminal_record(records, bookend=EARLIEST)

    def _extract_latest_record(self, records: list) -> dict:
        """
        Returns the most recent record for a particular site

        Parameters
        ----------
        records : list
            a list of records

        Returns
        -------
        dict
            the most recent record
        """
        return self._extract_terminal_record(records, bookend=LATEST)

    def read(
        self,
        site_record: SiteRecord | list,
        use_summarize: bool,
        start_ind: int,
        end_ind: int,
    ) -> (
        List[
            AnalyteRecord
            | AnalyteSummaryRecord
            | WaterLevelRecord
            | WaterLevelSummaryRecord
        ]
        | None
    ):
        """
        Returns a list of transformed parameter records. Transformed parameter records
        are standardized so that all of the records have the same format. They are
        defined in the record module. They behave just like a dictionary, but also have the
        to_row() method so that a record can be written to a table.

        If use_summarize is True, the summary of records for each site are returned.
        Otherwise, the cleaned and sorted records are returned for a site.

        Parameters
        ----------
        site_record : SiteRecord
            the site record(s) for the location whose parameter records are to be retrieved

        use_summarize : bool
            if True, the summary of records for each site are returned

        Returns
        --------
        list[AnalyteRecord | AnalyteSummaryRecord | WaterLevelRecord | WaterLevelSummaryRecord]
            a list of transformed parameter records
        """
        if isinstance(site_record, list):
            self.log(
                f"Gathering {self.name} summary for {len(site_record)} sites. {start_ind}-{end_ind}"
            )
        else:
            self.log(f"{site_record.id}: Gathering {self.name} data")

        all_analyte_records = self.get_records(site_record)
        if all_analyte_records:
            if not isinstance(site_record, list):
                site_record = [site_record]

            # return values
            ret = []

            # iterate over each site record and extract the parameter records for each site
            for site in site_record:
                site_records = self._extract_site_records(all_analyte_records, site)
                if not site_records:
                    self.warn(f"{site.id}: No records found")
                    continue
                # get cleaned records if _clean_records is defined by the source. This usually removes Nones/Null
                cleaned = self._clean_records(site_records)
                if not cleaned:
                    self.warn(f"{site.id} No clean records found")
                    continue

                if use_summarize:

                    # doesn't need to be returned, but can be used to debug/for development
                    kept_items = []
                    skipped_items = []

                    source_results = self._extract_source_parameter_results(cleaned)
                    source_units = self._extract_source_parameter_units(cleaned)
                    dates = self._extract_parameter_dates(cleaned)
                    source_names = self._extract_source_parameter_names(cleaned)

                    for source_result, source_unit, date, source_name in zip(
                        source_results, source_units, dates, source_names
                    ):
                        try:
                            converted_result, conversion_factor, warning_msg = (
                                convert_units(
                                    float(source_result),
                                    source_unit,
                                    self._get_output_units(),
                                    source_name,
                                    self.config.parameter,
                                    date,
                                )
                            )
                            if warning_msg == "":
                                kept_items.append(converted_result)
                            else:
                                msg = f"{warning_msg} for {site.id}"
                                self.warn(msg)
                        except TypeError:
                            skipped_items.append((site.id, source_result, source_unit))
                        except ValueError:
                            skipped_items.append((site.id, source_result, source_unit))

                    if len(skipped_items) > 0:
                        self.warn(
                            f"Skipped results because of formatting: {skipped_items}"
                        )

                    # if items is None or empty, no records were found or all results were None
                    if kept_items is not None and len(kept_items):
                        n = len(kept_items)

                        earliest_result = self._extract_earliest_record(cleaned)
                        latest_result = self._extract_latest_record(cleaned)
                        if not latest_result:
                            continue
                        rec = {
                            "nrecords": n,
                            "min": min(kept_items),
                            "max": max(kept_items),
                            "mean": sum(kept_items) / n,
                            "earliest_datetime": earliest_result["datetime"],
                            "earliest_value": earliest_result["value"],
                            "earliest_source_units": earliest_result[
                                "source_parameter_units"
                            ],
                            "earliest_source_name": earliest_result[
                                "source_parameter_name"
                            ],
                            "latest_datetime": latest_result["datetime"],
                            "latest_value": latest_result["value"],
                            "latest_source_units": latest_result[
                                "source_parameter_units"
                            ],
                            "latest_source_name": latest_result[
                                "source_parameter_name"
                            ],
                        }
                        transformed_record = self.transformer.do_transform(
                            rec,
                            site,
                        )
                        if transformed_record is None:
                            continue
                        else:
                            ret.append(transformed_record)
                else:
                    cleaned_sorted = [
                        self.transformer.do_transform(
                            self._extract_parameter(record), site
                        )
                        for record in cleaned
                        if self.transformer.do_transform(
                            self._extract_parameter(record), site
                        )
                        is not None
                    ]
                    if len(cleaned_sorted) == 0:
                        self.warn(f"{site.id}: No clean records found")
                        continue
                    cleaned_sorted = sorted(cleaned_sorted, key=self._sort_func)
                    ret.append((site, cleaned_sorted))
            return ret
        else:
            if isinstance(site_record, list):
                names = [str(r.id) for r in site_record]
            else:
                names = [str(site_record.id)]

            name = ",".join(names)
            self.warn(f"{name}: No records found")
            return None

    # ==========================================================================
    # Methods Implemented in BaseAnalyteSource and BaseWaterLevelSource
    # ==========================================================================

    def _validate_record(self, record: dict) -> None:
        """
        Determines that all standardized fields are present in the record.
        Raises a ValueError if any fields are missing from a record.

        For an analyte, the fields are
        - backend.constants.PARAMETER
        - backend.constants.PARAMETER_VALUE
        - backend.constants.PARAMETER_UNITS

        For a water level, the fields are
        - backend.constants.DTW
        - backend.constants.DTW_UNITS
        - backend.constants.DT_MEASURED

        Parameters
        ----------
        record : dict
            a record

        Returns
        -------
        None
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _validate_record"
        )

    def _get_output_units(self) -> str:
        """
        Determines the output units for the source from the configuration

        If the source is an analyte source, the output units are backend.config.Config.analyte_output_units
        If the source is a water level source, the output units are backend.config.Config.waterlevel_output_units
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _get_output_units"
        )

    # ==========================================================================
    # Methods That Need to be Implemented For Each Source
    # ==========================================================================

    def _extract_site_records(self, records: list[dict], site_record) -> list:
        """
        Returns all records for a single site as a list of records (which are dictionaries).

        Parameters
        ----------
        records : dict
            a dictionary of lists, where the keys are site ids and the values are parameter records

        site_record : dict
            the site record for the location whose parameter records are to be retrieved

        Returns
        -------
        list
            a list of records for the site
        """
        if site_record.chunk_size == 1:
            return records

        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_site_records"
        )

    def _clean_records(self, records: list) -> list:
        """
        Returns cleaned records if this function is defined for each source.
        Otherwise returns the records as-is.

        Parameters
        ----------
        records : list
            a list of records

        Returns
        -------
        list
            a list of cleaned records if this function is defined for each
            source. Otherwise returns the records as is.
        """
        return records

    def _extract_terminal_record(self, records, bookend):
        """
        Returns the terminal record for a particular site

        Parameters
        ----------
        records : list
            a list of records

        bookend : str
            determines if the first or last record is retrieved

        Returns
        -------
        dict
            the most recent record for every site
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_terminal_record"
        )

    def _extract_source_parameter_units(self, records: list) -> list:
        """
        Returns the units of the parameter records as a list, in the same order as the records themselves

        Parameters
        ----------
        records: list
            a list of parameter records

        Returns
        -------
        list
            a list of units for the parameter records in the same order as the records
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_source_parameter_units"
        )

    def _extract_parameter_dates(self, records: list) -> list:
        """
        Returns the dates of the parameter records as a list, in the same order as the records themselves

        Parameters
        ----------
        records: list
            a list of parameter records

        Returns
        -------
        list
            a list of dates for the parameter records in the same order as the records
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parameter_dates"
        )

    def _extract_source_parameter_names(self, records: list) -> list:
        """
        Returns the source names of the parameter records as a list, in the same order as the records themselves

        Parameters
        ----------
        records: list
            a list of parameter records

        Returns
        -------
        list
            a list of source names for the parameter records in the same order as the records
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_source_parameter_names"
        )

    def _extract_parameter_record(self, record: dict) -> dict:
        """
        Returns a parameter record with standardized fields added. This is only used for time series, not summary outputs

        For an analyte, the fields are
        - backend.constants.PARAMETER_NAME_SOURCE
        - backend.constants.PARAMETER_NAME_DIE
        - backend.constants.PARAMETER_VALUE
        - backend.constants.PARAMETER_UNITS

        Parameters
        ----------
        record: dict
            a parameter record

        Returns
        -------
        dict
            the parameter record with the fields added
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_parameter_record"
        )

    def _extract_source_parameter_results(self, records: list) -> list:
        """
        Returns the parameter results as a list from the records, in the same order as the records themselves. This is only used for summary outputs, not time serie

        Parameters
        ----------
        records: list
            a list of parameter records for a site

        Returns
        -------
        list
            a list of parameter results from the records, in the same order as the records
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} Must implement _extract_source_parameter_results"
        )

    def _extract_parameter(self, record: dict) -> dict:
        """
        Extracts a parameter record from a list of records. This is only used for time series, not summary outputs

        Parameters
        ----------
        record : dict
            a record

        Returns
        --------
        dict
            a record with the fields "datetime_measured", "parameter_value", "parameter_units", and "parameter" added
        """
        record = self._extract_parameter_record(record)
        self._validate_record(record)
        return record

    def _sort_func(self, x):
        """
        Sorting function to sort the records by date_measured

        Parameters
        ----------
        x : a record

        Returns
        -------
        datetime
            the date_measured of the record
        """
        return x.date_measured


class BaseAnalyteSource(BaseParameterSource):
    """
    Base class for all analyte sources.

    See BaseParameterSource for the methods that need to be implemented for each source
    """

    name = "analyte"

    def _get_output_units(self):
        return self.config.analyte_output_units

    def _validate_record(self, record):
        record[PARAMETER_NAME] = self.config.parameter
        for k in (PARAMETER_VALUE, PARAMETER_UNITS, DT_MEASURED):
            if k not in record:
                raise ValueError(f"Invalid record. Missing {k}")


class BaseWaterLevelSource(BaseParameterSource):
    """
    Base class for all water level sources.

    See BaseParameterSource for the methods that need to be implemented for each source
    """

    name = "water levels"

    def _get_output_units(self):
        return self.config.waterlevel_output_units

    def _extract_source_parameter_units(self, records):
        return [FEET for _ in records]

    def _validate_record(self, record):
        for k in (PARAMETER_VALUE, PARAMETER_UNITS, DT_MEASURED):
            if k not in record:
                raise ValueError(f"Invalid record. Missing {k}")


class BaseFileSource(BaseSource):
    """
    Base class for all file sources
    """

    name = "files"


# ============= EOF =============================================
