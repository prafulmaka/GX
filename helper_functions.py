# Import dependencies
from libraries import *

class HelperFunctions:
    def __init__(self, **kwargs):
        # Set default variable
        self.db_user = kwargs.get('db_user')
        self.db_pass = kwargs.get('db_pass')
        self.db_name_control = kwargs.get('db_name_control')
        self.db_name_data = kwargs.get('db_name_data')
        self.db_servername = kwargs.get('db_servername')
        self.jdbcPort = "1433"
        self.spark = kwargs.get('spark')

    def read_table(self, **kwargs):
        """
        Reads SQL Server table to a Spark dataframe.

        Parameters
        ----------
        query: The SELECT query to be executed on SQL Server
        db: Parameter to specify which database to read from. Default is MTConversion (Optional)

        Returns
        -------
        read_df: Spark dataframe created from executing SQL query

        """

        try:
            jdbcHostname = self.db_servername
            jdbcPort = self.jdbcPort
            jdbcDatabase = self.db_name_data
            spark = self.spark

            if 'db' in kwargs:
                jdbcDatabase = kwargs.get('db')

            jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(
                jdbcHostname, jdbcPort, jdbcDatabase)

            connectionProperties = {
                "user": self.db_user,
                "password": self.db_pass,
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }

            pushdown_query = f"({kwargs.get('query')}) Table_A"

            read_df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
            return read_df

        except Exception as e:
            print("Failed to read SQL query")

    def update_table(self, **kwargs):
        """
        Updates SQL Server table.

        Parameters
        ----------
        query: The UPDATE query to be executed on SQL Server
        db: Optional parameter to specify which database to read from. Default is MTConversion

        Returns
        -------
        N/A
        """

        try:
            query = kwargs.get('query')

            db_user = self.db_user
            db_pass = self.db_pass
            db_name = self.db_name_data
            db_servername = self.db_servername

            if 'db' in kwargs:
                db_name = kwargs.get('db')

            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                                  f'SERVER={db_servername};'
                                  f';UID={db_user};'
                                  f'PWD={db_pass}', DATABASE=db_name, autocommit=True)

            cursor = conn.cursor()
            cursor.execute(f"{query}")
            print("Update complete")

        except Exception as e:
            print("Failed to update SQL table: ", e)

    def create_expectation_suite(self, **kwargs):
        """
        Creates Expectation Suite using MSSQL metadata table (ddq_jobs_execution).

        Parameters
        ----------
        expectationsuite: Name of the Expectation Suite to be created


        Returns
        -------
        N/A
        """
        try:

            # Get validations from SQL
            query = "SELECT * FROM ddq.ddq_expectations"
            validation_list = self.read_table(query=query, db='great-exp')

            # Instantiate BaseDataContext
            context = ge.data_context.BaseDataContext(project_config=ValidationFrameworkInit().data_context_config)

            # Find unique Expectation Suite names
            for unique_exp_suite_row in validation_list.select('expectation_suite_name').distinct().collect():
                # Create Expectation Suite
                suite = context.create_expectation_suite(
                    expectation_suite_name=unique_exp_suite_row['expectation_suite_name'], overwrite_existing=True)
                # Use validations from SQL
                for row in validation_list.filter(validation_list.expectation_suite_name == unique_exp_suite_row[
                    'expectation_suite_name']).collect():
                    expectation_configuration = ExpectationConfiguration(
                        expectation_type=row['expectation_name'],
                        kwargs=json.loads(row['expectation_code']),
                        meta={
                            "schema_name": row['schema_name'],
                            "table_name": row['table_name']
                        }
                    )
                    # Add Expectation
                    suite.add_expectation(expectation_configuration=expectation_configuration)
                    context.save_expectation_suite(suite)

        except Exception as e:
            print("Failed to run create Expectation Suite: ", e)