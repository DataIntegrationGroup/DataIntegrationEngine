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
import click

from backend.persister import BasePersister, CSVPersister
from backend.transformer import BaseTransformer


class BaseSource:
    transformer_klass = BaseTransformer

    def __init__(self):
        self.transformer = self.transformer_klass()

    def log(self, msg):
        click.secho(f"{self.__class__.__name__:30s} {msg}", fg="yellow")

    def read(self, config, *args, **kw):
        self.log("Gathering records")
        n = 0
        for record in self.get_records(config):
            record = self.transformer.transform(record, config)
            if record:
                n += 1
                yield record

        self.log(f"nrecords={n}")

    def get_records(self, *args, **kw):
        raise NotImplementedError


# ============= EOF =============================================
