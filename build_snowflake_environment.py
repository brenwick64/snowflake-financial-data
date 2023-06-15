import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.
DDL_1_DIRECTORY_PATH = '/home/brenwick/snowflake-financial-data/DDL_1'
DDL_2_DIRECTORY_PATH = '/home/brenwick/snowflake-financial-data/DDL_2'
PYTHON_MODULES_PATH = '/home/brenwick/snowflake-financial-data/python_procs/modules'


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


def execute_file_ddl(session: Session, file_path):
    
    # Checks if file has contents
    if os.path.getsize(file_path):
        with open(file_path, encoding='utf8') as f:
            ddl_list = f.read().split('\n\n')
            
            # Executes each SQL statement found in file
            for ddl in ddl_list:
                result = session.sql(ddl).collect()
                print(f'DDL Result: {result}')
                
    else: 
        print(f'skipping {file_path}, no DDL found') 
    

def execute_folder_ddl(session: Session, directory: str):
    print(f'\nExecuting all SQL in {directory}\n')
    
    # Iterate through subfolders
    for subfolder in os.listdir(directory):
        subfolder_path = os.path.join(directory, subfolder)
        
        # Iterate through files
        for file_path in os.listdir(subfolder_path):
            file_path = os.path.join(subfolder_path, file_path)
            execute_file_ddl(session=session, file_path=file_path)
            
            
def upload_files_to_stage(session: Session, directory: str, stage_name: str):
    print(f'\nUploading files from {directory} to {stage_name}\n')
    
    # Iterates through modules
    for file_name in os.listdir(directory):
        suffix = file_name[len(file_name) - 3:]
        
        # Only uploads Python files
        if suffix == '.py':
            file_path = os.path.join(directory, file_name)
            result = session.file.put(local_file_name=file_path, stage_location=stage_name, overwrite=True)
            print(f'File Result: {result[0].status}: {file_name} To: {stage_name}')
            
          
def main(session: Session):
    execute_folder_ddl(session, DDL_1_DIRECTORY_PATH)
    upload_files_to_stage(session, PYTHON_MODULES_PATH, '@PYTHON_MODULES')
    execute_folder_ddl(session, DDL_2_DIRECTORY_PATH)
    

if __name__ == '__main__':
    main(snowflake_session)