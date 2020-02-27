import pandas as pd


class DataPrep:
    """Prepare sample dataFrame and load it's parquet file to s3"""

    def __init__(self, bucket, path_to_obj):
        """

        :param bucket: s3 bucket
        :param path_to_obj: s3 object path from bucket
        """
        self.s3_obj = f"s3://{bucket}/{path_to_obj}"

    @staticmethod
    def prep_data():
        """Prep sample dataFrame."""
        df = pd.DataFrame({
            'animals': ['falcon', 'dog', 'spider', 'fish'],
            'num_legs': [2, 4, 8, 0],
            'num_wings': [2, 0, 0, 0],
            'num_specimen_seen': [10, 2, 1, 8]}
        )
        return df

    @staticmethod
    def read_tx_data(path):
        return pd.read_csv(path)

    def load_to_s3(self, df):
        """
        convert dataframe to parquet and load to s3_path
        :param df: dataFrame to write
        :return:
        """
        df.to_parquet(self.s3_obj, engine="fastparquet", index=False, compression='gzip')
