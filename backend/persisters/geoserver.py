# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
import psycopg2

from backend.persister import BasePersister


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

        db = self.config.get('geoserver').get('db')
        try:
            self._connection = psycopg2.connect(
                dbname=db.get('dbname'),
                user=db.get('user'),
                password=db.get('password'),
                host=db.get('host'),
                port=db.get('port'),
            )
            self.log("Successfully connected to the database.")
        except psycopg2.Error as e:
            self.log(f"Failed to connect to the database: {e}", fg="red")


    def _write_to_db(self, records: list):
        """
        Write records to a PostgreSQL database.
        """
        # if not self._connection:
        #     self._connect()


        sources = {r.source for r in records}
        with self._connection.cursor() as cursor:
            for source in sources:
                # upsert sources
                sql = """INSERT INTO public.tbl_sources (name) VALUES (%s) ON CONFLICT (name) DO NOTHING"""
                cursor.execute(sql, (source,))
            self._connection.commit()

        with self._connection.cursor() as cursor:
            chunk_size = 100  # Adjust chunk size as needed
            # Process records in chunks
            keys= ["usgs_site_id", "alternate_site_id", "formation", "aquifer", "well_depth"]
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                print(f"Writing chunk {i // chunk_size + 1} of {len(records) // chunk_size + 1}")
                with self._connection.cursor() as cursor:
                    sql = """INSERT INTO public.tbl_location (name, properties, geometry, source_slug)
                             VALUES (%s, %s, public.ST_SetSRID(public.ST_MakePoint(%s, %s), 4326), %s) 
                             ON CONFLICT (name) DO UPDATE SET properties = EXCLUDED.properties;"""
                    values = [
                        (record.name, record.to_dict(keys), record.longitude, record.latitude, record.source)
                        for record in chunk
                    ]
                    cursor.executemany(sql, values)
                self._connection.commit()
            # for record in records:
            #     sql = """INSERT INTO public.tbl_location (name, properties, geometry, source_slug) VALUES (%s,%s,
            #     public.ST_SetSRID(public.ST_MakePoint(%s,%s), 4326),
            #     %s)"""
            #     # print(record)
            #     values = [record.name, record.properties,
            #               record.longitude, record.latitude, record.source]
            #     print(values)
            #     cursor.execute(sql, values)
            #
            # self._connection.commit()


# ============= EOF =============================================
