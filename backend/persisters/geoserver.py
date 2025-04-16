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

import psycopg2
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

from backend.persister import BasePersister

from sqlalchemy import Column, ForeignKey, create_engine, UUID, String, Integer
from geoalchemy2 import Geometry

Base = declarative_base()
#         dbname=db.get('dbname'),
#         user=db.get('user'),
#         password=db.get('password'),
#         host=db.get('host'),
#         port=db.get('port'),
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

    source = relationship("Sources", backref="locations")


class Sources(Base):
    __tablename__ = "tbl_sources"
    id = Column(Integer)
    name = Column(String, primary_key=True, index=True)


class GeoServerPersister(BasePersister):
    def __init__(self, *args, **kwargs):
        super(GeoServerPersister, self).__init__(*args, **kwargs)
        self._connection = None
        self._connect()

    def dump_sites(self, path: str):
        if self.sites:
            db = self.config.get('geoserver').get('db')
            dbname = db.get('db_name')
            self.log(f"dumping sites to {dbname}")
            self._write_to_db(self.sites)
        else:
            self.log("no sites to dump", fg="red")

    def _connect(self):
        """
        Connect to a PostgreSQL database on Cloud SQL.
        """
        sf = session_factory(self.config.get('geoserver').get('db'))
        self._connection = sf()

    def _write_to_db(self, records: list):
        """
        Write records to a PostgreSQL database in optimized chunks.
        """
        sources = {r.source for r in records}
        # with self._connection.cursor() as cursor:
        #     # Upsert sources
        #     for source in sources:
        #         sql = """INSERT INTO public.tbl_sources (name) VALUES (%s) ON CONFLICT (name) DO NOTHING"""
        #         cursor.execute(sql, (source,))
        #     self._connection.commit()
        with self._connection as conn:
            sql = insert(Sources).values([{"name": source} for source in sources]).on_conflict_do_nothing(
                index_elements=[Sources.name],)
            conn.execute(sql)

        chunk_size = 1000  # Larger chunk size for fewer commits
        keys = ["usgs_site_id", "alternate_site_id", "formation", "aquifer", "well_depth"]

        newrecords = []
        records = sorted(records, key=lambda r: str(r.id))
        for name, gs in groupby(records, lambda r: str(r.id)):
            gs = list(gs)
            n = len(gs)
            # print(f"Writing {n} records for {name}")
            if n>1:
                if n > len({r.source for r in gs}):
                    print("Duplicate source name found. Skipping...", name, [(r.name, r.source) for r in gs])
                    continue
            newrecords.extend(gs)
                    # break
                    # pass
                # print("Duplicate source name found. Skipping...", name, [r.source for r in gs])
                # break


        for i in range(0, len(newrecords), chunk_size):
            chunk = newrecords[i:i + chunk_size]
            print(f"Writing chunk {i // chunk_size + 1} of {len(records) // chunk_size + 1}")
            st = time.time()

            values = [
                {
                    "name": record.name,
                    "data_source_uid": record.id,
                    "properties": record.to_dict(keys),
                    "geometry": f"SRID=4326;POINT({record.longitude} {record.latitude})",
                    "source_slug": record.source,
                }
                for record in chunk
            ]

            # stmt = insert(Location).values(values).on_conflict_do_nothing()
            linsert = insert(Location)
            stmt = linsert.values(values).on_conflict_do_update(
                index_elements=[Location.data_source_uid],
                set_={"properties": linsert.excluded.properties}
            )

            with self._connection as conn:
                conn.execute(stmt)
                conn.commit()

            print('Chunk write time:', time.time() - st)

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
