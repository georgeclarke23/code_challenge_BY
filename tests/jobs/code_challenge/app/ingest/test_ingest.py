import pandas as pd
from pandas.testing import assert_frame_equal

from src.jobs.code_challenge.app.ingest.ingest import Ingest


class TestIngest:
    def test_should_get_csv_file(self, fake_context, monkeypatch, tmpdir):
        expected_df = pd.DataFrame({"Lovely": [1, 2], "Wonderful": [1, 2]})
        f = tmpdir.mkdir("data").join("hello.csv")
        expected_df.to_csv(f.strpath, index=False)
        monkeypatch.setenv("SOURCE_PATH", str(f.dirpath()))
        ingest = Ingest(fake_context).exec()
        actual_df = ingest.dataframe.toPandas()
        assert_frame_equal(left=actual_df, right=expected_df, check_dtype=False)
