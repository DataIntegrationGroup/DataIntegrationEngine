from click.testing import CliRunner
import pytest
from typing import List, Any

from backend.config import SOURCE_KEYS
from backend.constants import (
    WATERLEVELS,
    ARSENIC,
    BICARBONATE,
    CALCIUM,
    CARBONATE,
    CHLORIDE,
    FLUORIDE,
    MAGNESIUM,
    NITRATE,
    PH,
    POTASSIUM,
    SILICA,
    SODIUM,
    SULFATE,
    TDS,
    URANIUM,
)
from frontend.cli import weave


class BaseCLITestClass:

    runner: CliRunner
    agency: str
    no_agencies: List[str] = []

    @pytest.fixture(autouse=True)
    def setup(self):
        # STEUP CODE -----------------------------------------------------------
        self.runner = CliRunner()

        # turn off all sources except for the one being tested
        for source in SOURCE_KEYS:
            if source == self.agency:
                continue
            else:
                source_with_dash = source.replace("_", "-")
                self.no_agencies.append(f"--no-{source_with_dash}")

        # RUN TESTS ------------------------------------------------------------
        yield

        # TEARDOWN CODE ---------------------------------------------------------
        self.no_agencies = []

    def _test_weave(self, parameter, output):
        # Arrange
        arguments = [parameter, f"--output {output}", "--dry"]

        arguments.extend(self.no_agencies)

        print(arguments)

        # Act
        result = self.runner.invoke(weave, arguments)
        print(result.output)
        print(result.__dict__)

        # Assert
        assert result.exit_code == 0
