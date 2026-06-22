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
import logging
from logging.handlers import RotatingFileHandler
import os

import click


# Track handlers created by this module to avoid closing unrelated handlers
_managed_handlers = []


class Logger:
    """Standalone logger. Use make_logger() to create instances."""

    def __init__(self, name: str):
        self._name = name
        self.logger = logging.getLogger(name)

    def log(self, msg, level=None, fg="yellow", **kwargs):
        if level is None:
            level = logging.INFO
        click.secho(f"{self._name:40s}{msg}", fg=fg)
        self.logger.log(level, msg, **kwargs)

    def warn(self, msg, fg="red", **kwargs):
        self.log(msg, fg=fg, level=logging.WARNING, **kwargs)

    def debug(self, msg):
        self.log(msg, level=logging.DEBUG, fg="blue")


def make_logger(name: str) -> Logger:
    return Logger(name)


class Loggable:
    """Deprecated — do not subclass. Use make_logger() instead."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def log(self, msg, level=None, fg="yellow", **kwargs):
        if level is None:
            level = logging.INFO

        click.secho(f"{self.__class__.__name__:40s}{msg}", fg=fg)
        self.logger.log(level, msg, **kwargs)

    def warn(self, msg, fg="red", **kwargs):
        self.log(msg, fg=fg, level=logging.WARNING, **kwargs)

    def debug(self, msg):
        self.log(msg, level=logging.DEBUG, fg="blue")


def setup_logging(level=None, log_format=None, path=None):
    global _managed_handlers

    if level is None:
        level = logging.DEBUG
    if log_format is None:
        log_format = (
            "%(name)-40s: %(asctime)s %(levelname)-9s (%(threadName)-10s) %(message)s"
        )

    root = logging.getLogger()
    root.setLevel(level)

    # Remove only the RotatingFileHandler instances we created
    for handler in _managed_handlers:
        root.removeHandler(handler)
        handler.close()
    _managed_handlers.clear()

    if path is None:
        path = "die.log"
    else:
        path = os.path.join(path, "die.log")

    # shandler = logging.StreamHandler()
    rhandler = RotatingFileHandler(path, maxBytes=1e8, backupCount=50)
    _managed_handlers.append(rhandler)

    handlers = [rhandler]

    fmt = logging.Formatter(log_format)
    for hi in handlers:
        hi.setLevel(level)
        hi.setFormatter(fmt)
        root.addHandler(hi)


# ============= EOF =============================================
