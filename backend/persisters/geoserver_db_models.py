# ===============================================================================
# Author:  Jake Ross
# Copyright 2025 New Mexico Bureau of Geology & Mineral Resources
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# ===============================================================================
from geoalchemy2 import Geometry
from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, Date, Time
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()


def session_factory(connection: dict):
    user = connection.get("user", "postgres")
    password = connection.get("password", "")
    port = connection.get("port", 5432)
    database = connection.get("dbname", "gis")
    driver = connection.get("driver", "pg8000")

    url = f'postgresql+{driver}://'
    if connection.get("cloud_sql"):
        connector= Connector()
        instance_connection_name = connection.get("instance_connection_name")
        print("Connecting to Cloud SQL instance:", instance_connection_name)
        def get_conn():
            return connector.connect(
                instance_connection_name,
                'pg8000',
                user=user,
                password=password,
                db=database,
            )
        engine = create_engine(url, creator=get_conn)
    else:
        host = connection.get("host", "localhost")
        url = f"{url}{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(url)

    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Location(Base):
    __tablename__ = "tbl_location"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    data_source_uid = Column(String, index=True)

    properties = Column(JSONB)
    geometry = Column(Geometry(geometry_type="POINT", srid=4326))
    source_slug = Column(String, ForeignKey("tbl_sources.name"))

    source = relationship("Sources", backref="locations")


class Summary(Base):
    __tablename__ = "tbl_summary"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    data_source_uid = Column(String, index=True)

    properties = Column(JSONB)
    geometry = Column(Geometry(geometry_type="POINT", srid=4326))
    source_slug = Column(String, ForeignKey("tbl_sources.name"))
    parameter_slug = Column(String, ForeignKey("tbl_parameters.name"))

    source = relationship("Sources", backref="summaries")

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
# ============= EOF =============================================
