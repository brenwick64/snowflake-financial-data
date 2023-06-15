import os
from snowflake import snowpark
from dotenv import load_dotenv

from modules.TCBKController import TCBKController

load_dotenv()  # take environment variables from .env.
DDL_DIRECTORY_PATH = '/home/brenwick/snowflake-financial-data/DDL'
DML_DIRECTORY_PATH = '/home/brenwick/snowflake-financial-data/DML'


connection_parameters = {
  "account": os.getenv('SNOWFLAKE_ACCOUNT'),
  "user": os.getenv('SNOWFLAKE_USERNAME'),
  "password": os.getenv('SNOWFLAKE_PASSWORD'),
  "role": os.getenv('SNOWFLAKE_ROLE'),
  "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
  "database": os.getenv('SNOWFLAKE_DATABASE'),
  "schema": os.getenv('SNOWFLAKE_SCHEMA'),
}

snowflake_session = snowpark.Session.builder.configs(connection_parameters).create()

def main(session: snowpark.Session):
    controller = TCBKController()
    tcbk_statements_df = session.table('financial_data.public.tcbk_statements').to_pandas()
    tcbk_transaction_df = controller.create_transaction(tcbk_statements_df)
    print(tcbk_transaction_df.head())
    return session.create_dataframe(tcbk_transaction_df)

main(snowflake_session)