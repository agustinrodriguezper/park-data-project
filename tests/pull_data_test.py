from utils.api_utils import get_parks
from utils.data_utils import process_data
import pandas as pd


class TestPullDataClass:
    def test_df_shape(self):
        with open('./tests/data/park_data.csv', 'r+') as f:
            sample_data = pd.read_csv(f, index_col=False)
            sample_cols = sample_data.shape[1]
        parks, _ = get_parks(0, 100)
        processed_parks = process_data(parks)
        cols = processed_parks.shape[1]
        assert cols == sample_cols
