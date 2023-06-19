import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

BANKS = ['tcbk', 'boa', 'disc']
STATEMENTS_BASE_PATH = '/home/brenwick/snowflake-financial-data/statements'

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

def copy_stage_to_table(session: Session, bank_name: str):
    result = session.sql(f'COPY INTO {bank_name.upper()}_STATEMENTS FROM @{bank_name.upper()}_STATEMENT_STAGE FILE_FORMAT=CSV_FORMAT').collect()
    print(f'Status of {bank_name} Copy: {result[0].status}\n')
    
    
def upload_statement_to_stage(session: Session, bank_name: str):
    
    # Dynamic string construction
    stage_name = f'@{bank_name.upper()}_STATEMENT_STAGE'
    statement_folder_path = f'{STATEMENTS_BASE_PATH}/{bank_name.lower()}/'
    print(f'\nUploading files from /{bank_name}/ to {stage_name}\n')
    
    # Upload each file in folder
    for file_name in os.listdir(statement_folder_path):
        file_path = os.path.join(statement_folder_path, file_name)
        result = session.file.put(local_file_name=file_path, stage_location=stage_name)
        print(f'File Result: {result[0].status}: {file_name} To: {stage_name}')


def main(session: Session):
    for bank in BANKS:
        upload_statement_to_stage(session, bank)
        copy_stage_to_table(session, bank)
        
if __name__ == '__main__':
    main(snowflake_session)