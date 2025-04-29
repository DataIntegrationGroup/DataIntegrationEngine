# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import json
import os
import time
from itertools import groupby
from typing import Type
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.point import Point
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, Mapped


from backend.persister import BasePersister

from sqlalchemy import (
    Column,
    ForeignKey,
    create_engine,
    UUID,
    String,
    Integer,
    Float,
    Date,
    Time,
)
from geoalchemy2 import Geometry

Base = declarative_base()


def session_factory(connection: dict):
    user = connection.get("user", "postgres")
    password = connection.get("password", "")
    host = connection.get("host", "localhost")
    port = connection.get("port", 5432)
    database = connection.get("dbname", "gis")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionFactory


class Location(Base):
    __tablename__ = "tbl_location"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    data_source_uid = Column(String, index=True)

    properties = Column(JSONB)
    geometry = Column(Geometry(geometry_type="POINT", srid=4326))
    source_slug = Column(String, ForeignKey("tbl_sources.name"))

    source: Mapped["Sources"] = relationship(
        "Sources", backref="locations", uselist=False
    )


class Summary(Base):
    __tablename__ = "tbl_summary"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    data_source_uid = Column(String, index=True)

    properties = Column(JSONB)
    geometry = Column(Geometry(geometry_type="POINT", srid=4326))
    source_slug = Column(String, ForeignKey("tbl_sources.name"))
    parameter_slug = Column(String, ForeignKey("tbl_parameters.name"))

    source: Mapped["Sources"] = relationship(
        "Sources", backref="summaries", uselist=False
    )

    value = Column(Float)
    nrecords = Column(Integer)
    min = Column(Float)
    max = Column(Float)
    mean = Column(Float)

    latest_value = Column(Float)
    latest_date = Column(Date)
    latest_time = Column(Time)

    earliest_value = Column(Float)
    earliest_date = Column(Date)
    earliest_time = Column(Time)


class Parameters(Base):
    __tablename__ = "tbl_parameters"
    name = Column(String, primary_key=True, index=True)
    units = Column(String)


class Sources(Base):
    __tablename__ = "tbl_sources"
    id = Column(Integer)
    name = Column(String, primary_key=True, index=True)
    convex_hull = Column(Geometry(geometry_type="POLYGON", srid=4326))


class GeoServerPersister(BasePersister):
    def __init__(self, *args, **kwargs):
        super(GeoServerPersister, self).__init__(*args, **kwargs)
        self._connection = None
        self._connect()

    def dump_sites(self, path: str):
        if self.sites:
            db = self.config.get("geoserver").get("db")
            dbname = db.get("db_name")
            self.log(f"dumping sites to {dbname}")
            self._write_to_sites(self.sites)
        else:
            self.log("no sites to dump", fg="red")

    def dump_summary(self, path: str):
        if self.records:
            db = self.config.get("geoserver").get("db")
            dbname = db.get("db_name")
            self.log(f"dumping summary to {dbname}")
            self._write_to_summary(self.records)
        else:
            self.log("no records to dump", fg="red")

    def _connect(self):
        """
        Connect to a PostgreSQL database on Cloud SQL.
        """
        sf = session_factory(self.config.get("geoserver").get("db"))
        self._connection = sf()

    def _write_sources(self, records: list):
        sources = {r.source for r in records}
        with self._connection as conn:
            sql = (
                insert(Sources)
                .values([{"name": source} for source in sources])
                .on_conflict_do_nothing(
                    index_elements=[Sources.name],
                )
            )
            conn.execute(sql)
            conn.commit()

    def _write_sources_with_convex_hull(self, records: list):
        # sources = {r.source for r in records}
        with self._connection as conn:

            def key(r):
                return str(r.source)

            records = sorted(records, key=key)
            for source_name, group in groupby(records, key=key):
                source_records = list(group)
                # calculate convex hull for the source from the records

                # Create a MultiPoint object
                points = MultiPoint(
                    [
                        Point(record.longitude, record.latitude)
                        for record in source_records
                    ]
                )

                # Calculate the convex hull
                sinsert = insert(Sources)
                print("Writing source", source_name, points.convex_hull)
                sql = sinsert.values(
                    [{"name": source_name, "convex_hull": points.convex_hull.wkt}]
                ).on_conflict_do_update(
                    index_elements=[Sources.name],
                    set_={"convex_hull": sinsert.excluded.convex_hull},
                )
                # sql = insert(Sources).values([{"name": source,} for source in sources]).on_conflict_do_nothing(
                #     index_elements=[Sources.name],)
                conn.execute(sql)
            conn.commit()

    def _write_parameters(self):
        with self._connection as conn:
            sql = (
                insert(Parameters)
                .values(
                    [
                        {
                            "name": self.config.parameter,
                            "units": self.config.analyte_output_units,
                        }
                    ]
                )
                .on_conflict_do_nothing(
                    index_elements=[Parameters.name],
                )
            )
            print(sql)
            conn.execute(sql)
            conn.commit()

    def _write_to_summary(self, records: list):
        self._write_sources(records)
        self._write_parameters()
        for r in records:
            print(r, [r.to_dict()])
        keys = [
            "usgs_site_id",
            "alternate_site_id",
            "formation",
            "aquifer",
            "well_depth",
        ]

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
                    "earliest_time": (
                        record.earliest_time if record.earliest_time else None
                    ),
                }
                for record in chunk
            ]

            linsert = insert(Summary)
            return linsert.values(values).on_conflict_do_update(
                index_elements=[Summary.data_source_uid],
                set_={"properties": linsert.excluded.properties},
            )

        self._chunk_insert(make_stmt, records)

    def _chunk_insert(self, make_stmt, records: list, chunk_size: int = 10):
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            print(
                f"Writing chunk {i // chunk_size + 1} of {len(records) // chunk_size + 1}"
            )
            st = time.time()

            stmt = make_stmt(chunk)
            with self._connection as conn:
                conn.execute(stmt)
                conn.commit()

            print("Chunk write time:", time.time() - st)

    def _write_to_sites(self, records: list):
        """
        Write records to a PostgreSQL database in optimized chunks.
        """

        self._write_sources_with_convex_hull(records)

        keys = [
            "usgs_site_id",
            "alternate_site_id",
            "formation",
            "aquifer",
            "well_depth",
        ]
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
                set_={"properties": linsert.excluded.properties},
            )
            return stmt

        self._chunk_insert(make_stmt, records, chunk_size)

        #
        # newrecords = []
        # records = sorted(records, key=lambda r: str(r.id))
        # for name, gs in groupby(records, lambda r: str(r.id)):
        #     gs = list(gs)
        #     n = len(gs)
        #     # print(f"Writing {n} records for {name}")
        #     if n>1:
        #         if n > len({r.source for r in gs}):
        #             print("Duplicate source name found. Skipping...", name, [(r.name, r.source) for r in gs])
        #             continue
        #     newrecords.extend(gs)
        #             # break
        #             # pass
        #         # print("Duplicate source name found. Skipping...", name, [r.source for r in gs])
        #         # break
        #
        #
        # for i in range(0, len(newrecords), chunk_size):
        #     chunk = newrecords[i:i + chunk_size]
        #     print(f"Writing chunk {i // chunk_size + 1} of {len(records) // chunk_size + 1}")
        #     st = time.time()
        #
        #     values = [
        #         {
        #             "name": record.name,
        #             "data_source_uid": record.id,
        #             "properties": record.to_dict(keys),
        #             "geometry": f"SRID=4326;POINT({record.longitude} {record.latitude})",
        #             "source_slug": record.source,
        #         }
        #         for record in chunk
        #     ]
        #
        #     # stmt = insert(Location).values(values).on_conflict_do_nothing()
        #     linsert = insert(Location)
        #     stmt = linsert.values(values).on_conflict_do_update(
        #         index_elements=[Location.data_source_uid],
        #         set_={"properties": linsert.excluded.properties}
        #     )
        #
        #     with self._connection as conn:
        #         conn.execute(stmt)
        #         conn.commit()
        #
        #     print('Chunk write time:', time.time() - st)

        # # Pre-serialize properties to reduce processing time
        # values = [
        #     (record.name, json.dumps(record.to_dict(keys)), record.longitude, record.latitude, record.source)
        #     for record in chunk
        # ]
        #
        #     with self._connection.cursor() as cursor:
        #         sql = """INSERT INTO public.tbl_location (name, properties, geometry, source_slug)
        #                  VALUES (%s, %s, public.ST_SetSRID(public.ST_MakePoint(%s, %s), 4326), %s)
        #                  ON CONFLICT (name) DO UPDATE SET properties = EXCLUDED.properties;"""
        #         cursor.executemany(sql, values)
        #
        #     self._connection.commit()  # Commit once per chunk
        #     print('Chunk write time:', time.time() - st)
        #     break


# ============= EOF =============================================
