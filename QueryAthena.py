import time
import boto3
import pandas as pd
from DataPrep import DataPrep


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

    def get_query_output_path(self, exec_id):
        return f"{self.s3_output}{exec_id}.csv"

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
    data_folder_path = 'sample_txn'
    s3_obj_dir_path = f"s3://{bucket}/{data_folder_path}/"
    output_location = f's3://{bucket}/output/'

    # prepare sample data
    a = DataPrep(bucket=bucket, path_to_obj=f'{data_folder_path}/new_test.parquet')

    import os
    cwd = os.getcwd()
    path = os.path.join(os.getcwd(), "sample-data/txn_list.csv")
    df = a.read_tx_data(path)

    # load sample data as parquet format
    a.load_to_s3(df)

    # ddl to create table from s3_obj
    create_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `tx_list`(
      `txn_id` string, 
      `ref_txn_id` string, 
      `card_hash` string, 
      `account_id` string, 
      `user_id` string, 
      `full_name` string, 
      `business_id` string, 
      `business_name` string, 
      `balance_id` string, 
      `processor` string, 
      `fraud_txn_id` string, 
      `mcc` string, 
      `txn_memo` string, 
      `amount` decimal(10,0), 
      `txn_type` string, 
      `tx_src_id` string, 
      `tx_source` string, 
      `ledger_entry` decimal(10,0), 
      `currency` string, 
      `base_fx` decimal(10,0), 
      `base_breakage` decimal(10,0), 
      `user_amount` decimal(10,0), 
      `base_currency` decimal(10,0), 
      `user_currency` string, 
      `txn_currency` decimal(10,0), 
      `status` string, 
      `decline_sub_status` string, 
      `permissible_action` string, 
      `txn_amount_finality` string, 
      `card_type` string, 
      `tx_src_fullname` string, 
      `tx_src_phone` decimal(10,0), 
      `tx_src_email` decimal(10,0), 
      `updated_at` decimal(10,0), 
      `created_at` decimal(10,0), 
      `tracking_id` string, 
      `from_clabe` decimal(10,0), 
      `to_clabe` decimal(10,0), 
      `sender_bank_id` decimal(10,0))
    ROW FORMAT SERDE 
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
    STORED AS INPUTFORMAT 
      'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      '{s3_obj_dir_path}'
    TBLPROPERTIES (
      'has_encrypted_data'='true', 
      'transient_lastDdlTime'='1582817907')
"""

    # process table data as necessary and select it
    get_data = """
        select 
         account_id, full_name, sum(amount) as total_transaction_amount
        from tx_list
        where status='APPROVED'
         and created_at > 1569320766000
        
         group by account_id, full_name
         order by total_transaction_amount desc;
         
    """
    qa = QueryAthena(database='db_test', bucket=bucket, output_location=output_location)

    # create aws Athena table from s3_bucket
    create_id = qa.run_query(create_query)

    # exec select query
    q1_id = qa.run_query(get_data)
    q1path = qa.get_query_output_path(q1_id)

    print(f"Query {get_data} csv s3_path: {q1path}")
