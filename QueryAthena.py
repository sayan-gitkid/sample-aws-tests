import time
import boto3
import pandas as pd
from athena.DataPrep import DataPrep


class QueryAthena:
    """
    Execute provided query in AWS athena.
    Provides function to fetch query result.
    """
    def __init__(self, database, bucket, output_location):
        self.database = database
        self.bucket = bucket
        # folder to store query meta_data
        self.s3_output = output_location
        self.client = boto3.client('athena')

    def start_exec(self, q):
        """
        Start executing query
        :param q: provided query
        :return:
        """
        try:

            response = self.client.start_query_execution(
                QueryString=q,
                QueryExecutionContext={
                    'Database': self.database
                },
                ResultConfiguration={
                    'OutputLocation': self.s3_output,
                }
            )
            return response
        except Exception as e:
            print(e)

    def run_query(self, query):
        """
        Exec query and check status
        :param query:
        :return: query exec id
        """
        try:

            res = self.start_exec(query)
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = \
                    self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution'][
                        'Status'][
                        'State']
                print(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query))
                time.sleep(10)
            print('Query "{}" finished.'.format(query))
            return res['QueryExecutionId']

        except Exception as e:
            print(e)

    def obtain_data(self, filename):
        """
        Query output is always saved as csv in s3 bucket specified in 'OutputLocation'
        Reads the csv of the query result
        :param filename: file name is exec id of the query
        :return: dataFrame of the read csv.
        """
        try:
            s3_path = f"{self.s3_output}{filename}.csv"
            return pd.read_csv(s3_path)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    bucket = 'test-data-lake-feb-2020-parquet'
    data_folder_path = 'tb2'
    s3_obj_dir_path = f"s3://{bucket}/{data_folder_path}/"
    output_location = f's3://{bucket}/output/'

    # prepare sample data
    a = DataPrep(bucket=bucket, path_to_obj=f'{data_folder_path}/new_test.parquet')
    df = a.prep_data()

    # load sample data as parquet format
    a.load_to_s3(df)

    # ddl to create table from s3_obj
    create_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS db_test.tba (
      `animals` string,
      `num_legs` smallint,
      `num_wings` smallint,
      `num_specimen_seen` smallint 
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES (
      'serialization.format' = '1'
    ) LOCATION '{s3_obj_dir_path}'
    TBLPROPERTIES ('has_encrypted_data'='true');
"""

    # process table data as necessary and select it
    get_data = """
        SELECT * FROM "db_test"."tba" limit 10;
    """
    qa = QueryAthena(database='db_test', bucket=bucket, output_location=output_location)

    # create aws Athena table from s3_bucket
    create_id = qa.run_query(create_query)

    # exec select query
    data_id = qa.run_query(get_data)

    # fetch data from sql query
    df = qa.obtain_data(data_id)  # process data as necessary
    print(df.head())
    print("process data as necessary")


