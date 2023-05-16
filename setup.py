import os
from snowflake import snowpark
from snowflake.snowpark import Session
from dotenv import load_dotenv

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

my_session = Session.builder.configs(connection_parameters).create()


def create_tables(session: snowpark.Session):
    sql_text = ''' 
                CREATE OR REPLACE TABLE TEST (
                customer_id int,
                first_name varchar,
                last_name varchar
                );
          '''
    print(session.sql(sql_text).collect())
    
def drop_tables(session: snowpark.Session):
    sql_text = '''
                DROP TABLE TEST;
         '''
    print(session.sql(sql_text).collect())

def main(session: snowpark.Session):
    
    #  # Your code goes here, inside the "main" handler.
    # tableName = 'INFORMATION_SCHEMA.APPLICABLE_ROLES'
    # #dataframe = session.table(tableName).filter(col("language") == 'python')
    # dataframe = session.table(tableName)

    # # Print a sample of the dataframe to standard output.
    # dataframe.show()

    # Return value will appear in the Results tab.
    create_tables(session)
    drop_tables(session)


main(my_session)