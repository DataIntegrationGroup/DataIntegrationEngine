# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import time
from itertools import groupby
from typing import Callable

from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.point import Point
from sqlalchemy.dialects.postgresql import insert

from backend.persister import BasePersister

from backend.persisters.geoserver_db_models import session_factory, Location, Summary, Parameters, Sources

class GeoServerPersister(BasePersister):
    def __init__(self, *args, **kwargs):
        super(GeoServerPersister, self).__init__(*args, **kwargs)
        self._connection = None
        self._connect()

    def dump_sites(self, path: str = None):
        if self.sites:
            db = self.config.get('geoserver').get('db')
            dbname = db.get('db_name')
            self.log(f"dumping sites to {dbname}")
            self._write_to_sites(self.sites)
        else:
            self.log("no sites to dump", fg="red")

    def dump_summary(self, path: str = None):
        if self.records:
            db = self.config.get('geoserver').get('db')
            dbname = db.get('db_name')
            self.log(f"dumping summary to {dbname}")
            self._write_to_summary(self.records)
        else:
            self.log("no records to dump", fg="red")

    def _connect(self):
        """
        Connect to a PostgreSQL database on Cloud SQL.
        """
        sf = session_factory(self.config.get('geoserver').get('db'))
        self._connection = sf()

    def _write_sources(self, records: list):
        sources = {r.source for r in records}
        with self._connection as conn:
            sql = insert(Sources).values([{"name": source} for source in sources]).on_conflict_do_nothing(
                index_elements=[Sources.name],)
            conn.execute(sql)
            conn.commit()

    def _write_sources_with_convex_hull(self, records: list):
        # sources = {r.source for r in records}
        with self._connection as conn:
            def key(r):
                return str(r.source)

            records = sorted(records, key=key)
            for source_name, group in groupby(records, key=key):
                group = list(group)
                # calculate convex hull for the source from the records

                # Create a MultiPoint object
                points = MultiPoint([Point(record.longitude, record.latitude) for record in group])

                # Calculate the convex hull
                sinsert = insert(Sources)
                print("Writing source", source_name, points.convex_hull)
                sql = sinsert.values([{"name": source_name,
                                               "convex_hull": points.convex_hull.wkt}]).on_conflict_do_update(
                    index_elements=[Sources.name],
                    set_={"convex_hull": sinsert.excluded.convex_hull})
                # sql = insert(Sources).values([{"name": source,} for source in sources]).on_conflict_do_nothing(
                #     index_elements=[Sources.name],)
                conn.execute(sql)
            conn.commit()

    def _write_parameters(self):
        with self._connection as conn:
            sql = insert(Parameters).values([{"name": self.config.parameter,
                                              "units": self.config.analyte_output_units}]).on_conflict_do_nothing(
                index_elements=[Parameters.name],)
            conn.execute(sql)
            conn.commit()

    def _write_to_summary(self, records: list):
        self._write_sources(records)
        self._write_parameters()
        for r in records:
            print(r, [r.to_dict()])
        keys = ["usgs_site_id", "alternate_site_id", "formation", "aquifer", "well_depth"]
        def make_stmt(chunk):
            values = [
                {
                    "name": record.location,
                    "data_source_uid": record.id,
                    "properties": record.to_dict(keys),
                    "geometry": f"SRID=4326;POINT({record.longitude} {record.latitude})",
                    "source_slug": record.source,
                    "parameter_slug": self.config.parameter,
                    "nrecords": record.nrecords,
                    "min": record.min,
                    "max": record.max,
                    "mean": record.mean,
                    "latest_value": record.latest_value,
                    "latest_date": record.latest_date,
                    "latest_time": record.latest_time if record.latest_time else None,
                    "earliest_value": record.earliest_value,
                    "earliest_date": record.earliest_date,
                    "earliest_time": record.earliest_time if record.earliest_time else None,
                }
                for record in chunk
            ]

            linsert = insert(Summary)
            return linsert.values(values).on_conflict_do_update(
                index_elements=[Summary.data_source_uid],
                set_={"properties": linsert.excluded.properties}
            )

        self._chunk_insert(make_stmt, records)

    def _write_to_sites(self, records: list):
        """
        Write records to a PostgreSQL database in optimized chunks.
        """

        self._write_sources_with_convex_hull(records)

        keys = ["usgs_site_id", "alternate_site_id", "formation", "aquifer", "well_depth"]
        chunk_size = 1000  # Larger chunk size for fewer commits

        def make_stmt(chunk):
            values = [
                {
                    "name": record.location,
                    "data_source_uid": record.id,
                    "properties": record.to_dict(keys),
                    "geometry": f"SRID=4326;POINT({record.longitude} {record.latitude})",
                    "source_slug": record.source,
                }
                for record in chunk
            ]
            linsert = insert(Location)
            stmt = linsert.values(values).on_conflict_do_update(
                index_elements=[Location.data_source_uid],
                set_={"properties": linsert.excluded.properties}
            )
            return stmt

        self._chunk_insert(make_stmt, records, chunk_size)

    def _chunk_insert(self, make_stmt: Callable, records: list, chunk_size: int = 10):
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            print(f"Writing chunk {i // chunk_size + 1} of {len(records) // chunk_size + 1}")
            st = time.time()

            stmt = make_stmt(chunk)
            with self._connection as conn:
                conn.execute(stmt)
                conn.commit()

            print('Chunk write time:', time.time() - st)

# ============= EOF =============================================
