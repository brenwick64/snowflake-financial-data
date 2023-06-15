import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

TCBK_STATEMENT_PATH = '/home/brenwick/snowflake-financial-data/statements/tcbk'

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

def temp_execute_copy(session: Session):
    result = session.sql('COPY INTO TCBK_STATEMENTS FROM @TCBK_STATEMENT_STAGE FILE_FORMAT=CSV_FORMAT').collect()
    print(f'TCBK Stage Files: {result[0].status}') 

def main(session: Session):
    stage_name = '@TCBK_STATEMENT_STAGE'
    print(f'\nUploading files from {TCBK_STATEMENT_PATH} to {stage_name}\n')
    for file_name in os.listdir(TCBK_STATEMENT_PATH):
        file_path = os.path.join(TCBK_STATEMENT_PATH, file_name)
        result = session.file.put(local_file_name=file_path, stage_location=stage_name)
        print(f'File Result: {result[0].status}: {file_name} To: {stage_name}')
    temp_execute_copy(session)
    
if __name__ == '__main__':
    main(snowflake_session)