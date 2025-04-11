from tests.test_cli import BaseCLITestClass

class TestNMBGMRCLI(BaseCLITestClass):
    """Test the CLI for the NMBGMR source."""

    agency = "nmbgmr-amp"

    def test_weave(self):
        # Test the weave command for NMBGMR
        self._test_weave(
            parameter="waterlevels",
            output="summary"
        )