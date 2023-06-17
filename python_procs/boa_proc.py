import os
from snowflake import snowpark
from dotenv import load_dotenv

from modules.BOAController import BOAController

load_dotenv()  # take environment variables from .env.

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
    controller = BOAController()
    boa_statements_df = session.table('financial_data.public.boa_statements').to_pandas()
    boa_transaction_df = controller.create_transaction(boa_statements_df)
    print(boa_transaction_df.head())
    return session.create_dataframe(boa_transaction_df)


main(snowflake_session)