# ===============================================================================
# Copyright 2024 ross
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

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup(
    name="nmuwd",
    version="0.9.7",
    author="Jake Ross",
    description="New Mexico Water Data Integration Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DataIntegrationGroup/DataIntegrationEngine",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "flask",
        "frost_sta_client",
        "Geoalchemy2",
        "geopandas",
        "google-cloud-storage",
        "gunicorn",
        "httpx",
        "mypy",
        "pandas",
        "psycopg2",
        "pytest",
        "pyyaml",
        "types-pyyaml",
        "urllib3>=2.2.0,<3.0.0",
    ],
    entry_points={
        "console_scripts": [
            "die = frontend.cli:cli",
        ],
    },
    packages=["frontend", "backend"]
    + [f"backend.{p}" for p in find_packages("backend")],
    python_requires=">=3.6",
    include_package_data=True,
    # package_data={
    # If any package contains *.txt or *.rst files, include them:
    #     "templates": [
    #         "*.template",
    #     ],
    # },
)
# ============= EOF =============================================
