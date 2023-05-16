import os
from snowflake import snowpark
from snowflake.snowpark import Session
from dotenv import load_dotenv

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

snowflake_session = Session.builder.configs(connection_parameters).create()


def execute_all_sql(session: snowpark.Session, directory: str):
    print(f'\nExecuting all SQL in {directory}\n')
    # Split all files out of main DDL directory
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        
        # Check if file is empty
        if os.path.getsize(filepath):
            with open(filepath, encoding='utf8') as f:
                sql_list = f.read().split('\n\n')
                
                # Execute each SQL statement & print result
                for sql in sql_list:
                    result = session.sql(sql).collect()
                    print(result)
        else:
            print(f'skipping {filename}, no DDL found')
    
                        
def main(session: snowpark.Session):
    execute_all_sql(session, DDL_DIRECTORY_PATH)
    execute_all_sql(session, DML_DIRECTORY_PATH)

if __name__ == '__main__':
    main(snowflake_session)