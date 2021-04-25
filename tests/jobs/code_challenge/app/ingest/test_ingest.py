import csv
import pandas as pd
from pandas.testing import assert_frame_equal
from tempfile import NamedTemporaryFile, TemporaryDirectory

from src.jobs.code_challenge.app.ingest.ingest import Ingest
from tests.base import PySparkTest


class TestIngest(PySparkTest):
    def test_should_get_csv_file(self,  monkeypatch):
        expected_df = pd.DataFrame({"trip": [5, 6]})
        monkeypatch.setenv("SOURCE_PATH", "test.csv")
        with TemporaryDirectory() as tmpdirname:
            with NamedTemporaryFile(dir=tmpdirname, mode='w', delete=False, suffix='.csv') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(["trip"])
                csv_writer.writerow([5])
                csv_writer.writerow([6])
                ingest = Ingest(self.context).exec()
                actual_df = ingest.dataframe.toPandas()
        assert_frame_equal(left=actual_df, right=expected_df, check_dtype=False)
