from helper_functions import HelperFunctions
from gx_initialization import ValidationFrameworkInit
from libraries import *

class ValidationFrameworkExecute:
    def __init__(self, **kwargs):
        # Set default variable
        self.db_user = kwargs.get('db_user')
        self.db_pass = kwargs.get('db_pass')
        self.db_pass_quote = kwargs.get('db_pass_quote')
        self.db_name_control = kwargs.get('db_name_control')
        self.db_name_data = kwargs.get('db_name_data')
        self.db_servername = kwargs.get('db_servername')
        self.jdbcPort = "1433"
        self.spark = kwargs.get('spark')

        # Set db_name variable if kwarg exists
        if 'db' in kwargs:
            self.db_name_data = kwargs.get('db')

        # Instantiate HelperFunctions()
        self.helperfunctions = HelperFunctions(
            db_user=self.db_user,
            db_pass=self.db_pass,
            db_name_control=self.db_name_control,
            db_name_data=self.db_name_data,
            db_servername=self.db_servername,
            spark = self.spark
        )

        # Instantiate ValidationFrameworkInit()
        self.validationframeworkinit = ValidationFrameworkInit(
            db_user=self.db_user,
            db_pass=self.db_pass,
            db_pass_quote=self.db_pass_quote,
            db_name_control=self.db_name_control,
            db_name_data=self.db_name_data,
            db_servername=self.db_servername,
            jdbcPort=self.jdbcPort
        )

    def run_expectation_suite(self, **kwargs):
        """
        Runs Expectation Suite. This method runs Expectation Suites using a Checkpoint and a RuntimeBatchRequest.
        Includes steps to update job metrics in ControlDB tables (ddq_jobs_execution, ddq_jobs_execution_details) and also loads failed records as Delta/Parquet format files to Blob storage.

        Parameters
        ----------
        N/A

        Returns
        -------
        N/A

        """
        try:
            # Save start time
            start_time = time.strftime('%Y-%m-%d %H:%M:%S.%M')

            # Get job execution details from SQL
            query = "SELECT * FROM ddq.ddq_jobs_execution WHERE (job_status IS NULL)"
            ddq_job_exec = self.helperfunctions.read_table(query=query, db=self.db_name_control)

            # Get Expectation config details from SQL
            # Inner join DDQ_EXPECTATIONS_CONFIG x DDQ_EXPECTATIONS
            query = """
      SELECT A.config_id, A.exp_id, A.schema_name, A.table_name, A.column_name, A.expectation_parameters, B.expectation_name
      FROM ddq.ddq_expectations_config A
      INNER JOIN ddq.ddq_expectations B ON (A.exp_id = B.exp_id)
      WHERE A.flag = 1
      """
            ddq_expec = self.helperfunctions.read_table(query=query, db=self.db_name_control)

            # Join DDQ_EXPECTATIONS x DDQ_JOBS_EXECUTION
            expec_jobs = ddq_expec.join(ddq_job_exec, on=['schema_name', 'table_name'], how='inner')

            # Instantiate BaseDataContext
            context = ge.data_context.BaseDataContext(project_config=self.validationframeworkinit.data_context_config)

            # Get unique rows based on schema name, table name, job name
            # Ensure column name, expectation name is not null
            unique_rows = expec_jobs.filter(
                (expec_jobs.column_name.isNotNull()) & (expec_jobs.expectation_name.isNotNull())) \
                .dropDuplicates(subset=['schema_name', 'table_name', 'job_name'])
            # Create Expectation Suite
            for unique_row in unique_rows.collect():
                suite = context.create_expectation_suite(expectation_suite_name=unique_row['job_name'],
                                                         overwrite_existing=True)

            # Create StoreMetricsAction list
            new_action = {
                "name": "store_metrics",
                "action": {
                    "class_name": "StoreMetricsAction",
                    "target_store_name": "metric_store",
                    "requested_metrics": {
                        "*": [
                            "statistics.evaluated_expectations",
                            "statistics.success_percent",
                            "statistics.successful_expectations",
                            "statistics.unsuccessful_expectations"
                        ]
                    }
                }
            }

            for unique_row in expec_jobs.select('job_name').distinct().collect():
                new_action["action"]["requested_metrics"][unique_row["job_name"]] = []
            for row in expec_jobs.collect():
                # Add metric action to new_action
                new_action["action"]["requested_metrics"][row["job_name"]].append(
                    {"column": {row["column_name"]: [f"{row['expectation_name']}.result.element_count",
                                                     f"{row['expectation_name']}.result.unexpected_count",
                                                     f"{row['expectation_name']}.result.observed_value",
                                                     f"{row['expectation_name']}.success"]}})

            # Add Expectation to Expectation Suite
            for row in expec_jobs.collect():
                expectation_config = ExpectationConfiguration(
                    expectation_type=row['expectation_name'],
                    kwargs=json.loads(row['expectation_parameters']),
                    meta={
                        "schema_name": row['schema_name'],
                        "table_name": row['table_name'],
                        "column_name": row['column_name']
                    }
                )

                # Get Expectation Suite
                suite = context.get_expectation_suite(expectation_suite_name=row['job_name'])
                # Add
                suite.add_expectation(expectation_configuration=expectation_config)
                # Save
                context.save_expectation_suite(expectation_suite=suite)

            # Applying patch to override config variable error
            # This should be temporary while GitHub ticket is raised
            from great_expectations.data_context import util

            def substitute_config_variable_patch(template_str, config_variables_dict,
                                                 dollar_sign_escape_string: str = r"\$"):

                return template_str

            util.substitute_config_variable = substitute_config_variable_patch

            # Create batch request
            for row in unique_rows.collect():
                # Create query
                # Incorporate full_table flag
                if row.full_table == 0:
                    query = f"SELECT * FROM {row['schema_name']}.{row['table_name']} WHERE [{row['pipeline_run_id_name']}] = '{row['pipeline_run_id']}'"
                if row.full_table == 1:
                    query = f"SELECT * FROM {row['schema_name']}.{row['table_name']}"
                # Continue batch request
                batch_request = RuntimeBatchRequest(
                    datasource_name='my_spark_datasource_config',
                    data_connector_name='default_runtime_data_connector_name',
                    data_asset_name=f"{row['job_name']}: {row['schema_name']}.{row['table_name']}",
                    runtime_parameters={
                        "query": query
                    },
                    batch_identifiers={
                        "batchid": time.time()
                    }
                )

                # Create checkpoint config
                checkpoint_config = {
                    "class_name": "SimpleCheckpoint",
                    "validations": [
                        {
                            "batch_request": batch_request,
                            "expectation_suite_name": row['job_name']
                        }
                    ]
                }

                # Create SimpleCheckpoint
                checkpoint = SimpleCheckpoint(
                    name=f"Checkpoint: {row['job_name']}",
                    data_context=context,
                    **checkpoint_config
                )

                # Add action list with metrics
                checkpoint.action_list.append(new_action)

                # Run checkpoint
                checkpoint_result = checkpoint.run(run_name=batch_request.id)

                # Update only run id details first
                # in DDQ_JOBS_EXECUTION table
                end_time = time.strftime('%Y-%m-%d %H:%M:%S.%M')

                query = f"UPDATE ddq.ddq_jobs_execution SET run_id = '{batch_request.id}' WHERE job_name = '{row['job_name']}'"
                self.helperfunctions.update_table(query=query, db=self.db_name_control)

                # Update status_msg in DDQ_JOBS_EXECUTION
                self.update_execution_details(batchid=batch_request.id, expec_jobs=expec_jobs)

                # Get failed records as a dataframe
                # This step uploads to Azure Blob storage
                self.get_failed_records_details(batchid=batch_request.id)

                # Update job_status, execution_start_time, execution_end_time
                # in DDQ_JOBS_EXECUTION table
                query = f"UPDATE ddq.ddq_jobs_execution SET job_status = 'Success', execution_start_time = '{start_time}', execution_end_time = '{end_time}' WHERE (run_id = '{batch_request.id}') AND (job_name = '{row['job_name']}')"
                self.helperfunctions.update_table(query=query, db=self.db_name_control)

                print("Run Expectation Suite is complete")

            return

        except Exception as e:
            print("Failed to run Expectation Suite: ", e)

    def update_execution_details(self, **kwargs):
        """
        Updates statistics/metrics in the Execution Details table.

        Parameters
        ----------
        batchid: The Great Expectations batchid/runname/runid
        expecjobs: Spark df named expec_jobs

        Returns
        -------
        N/A

        """
        try:
            # Update Successful Expectations and Unsuccessful Expectations
            # In the DDQ_JOBS_EXECUTION table
            # Query and save DDQ_JOBS_EXECUTION and GE_METRICS
            batch_id = kwargs.get('batchid')
            expec_jobs = kwargs.get('expec_jobs')

            jobsquery = f"SELECT * FROM ddq.ddq_jobs_execution WHERE run_id = '{batch_id}' AND (status_msg IS NULL)"
            metricsquery = f"SELECT * FROM dbo.ge_metrics WHERE run_name = '{batch_id}'"
            jobs = self.helperfunctions.read_table(query=jobsquery, db=self.db_name_control)
            metrics = self.helperfunctions.read_table(query=metricsquery, db=self.db_name_control)

            # Inner-join DDQ_JOBS_EXECUTION x GE_METRICS
            metrics_jobs = metrics.join(jobs, on=[(metrics.run_name == jobs.run_id),
                                                  (metrics.expectation_suite_identifier == jobs.job_name)], how='inner')

            # Save metrics
            # Save statistics.successful_expectations, statistics.unsuccessful_expectations
            success_exp_count = metrics_jobs.filter(
                metrics_jobs.metric_name == "statistics.successful_expectations").select('value').collect()
            success_exp_count = json.loads(success_exp_count[0]['value'])['value']

            exp_count = metrics_jobs.filter(metrics_jobs.metric_name == "statistics.evaluated_expectations").select(
                'value').collect()
            exp_count = json.loads(exp_count[0]['value'])['value']

            # Update DDQ_JOBS_EXECUTION table
            query = f"UPDATE ddq.ddq_jobs_execution SET status_msg = '{success_exp_count}/{exp_count} Validations Passed' WHERE (run_id = '{batch_id}') AND (status_msg IS NULL)"
            self.helperfunctions.update_table(query=query, db=self.db_name_control)

            # Update Expectation details
            # Expectation details to be loaded to DDQ_JOBS_EXECUTION_DETAILS
            metrics_details = expec_jobs.join(metrics, on=[(expec_jobs.run_id == metrics.run_name), (
                        expec_jobs.expectation_name == split(metrics.metric_name, '[.]').getItem(0)), (
                                                                       expec_jobs.column_name == split(
                                                                   metrics.metric_kwargs_id, '[=]').getItem(1))],
                                              how='inner')

            # Create metric_split column
            # Which splits the metric_name column on [.] and creates array
            metrics_details = metrics_details.withColumn("metric_split", split(metrics_details.metric_name, '[.]'))

            # Create additional columns
            # expectation_status, scanned, success, failure, details
            metrics_details = metrics_details.withColumn("expectation_status",
                                                         when(element_at(col('metric_split'), -1) == "success",
                                                              metrics_details.value)) \
                .withColumn("scanned",
                            when(element_at(col('metric_split'), -1) == "element_count", metrics_details.value)) \
                .withColumn("success",
                            when(element_at(col('metric_split'), -1) == "element_count", metrics_details.value)) \
                .withColumn("failure",
                            when(element_at(col('metric_split'), -1) == "unexpected_count", metrics_details.value)) \
                .withColumn("details",
                            when(element_at(col('metric_split'), -1) == "observed_value", metrics_details.value))

            # Format columns in metrics_details df
            # Get value from json strings
            metrics_details = metrics_details.withColumn("expectation_status",
                                                         when(col("expectation_status").isNotNull(),
                                                              get_json_object(col("expectation_status"), "$.value")))
            metrics_details = metrics_details.withColumn("scanned", when(col("scanned").isNotNull(),
                                                                         get_json_object(col("scanned"), "$.value")))
            metrics_details = metrics_details.withColumn("success", when(col("success").isNotNull(),
                                                                         get_json_object(col("success"), "$.value")))
            metrics_details = metrics_details.withColumn("failure", when(col("failure").isNotNull(),
                                                                         get_json_object(col("failure"), "$.value")))
            metrics_details = metrics_details.withColumn("details", when(col("details").isNotNull(),
                                                                         get_json_object(col("details"), "$.value")))

            # Further format columns by getting rid of nulls
            metrics_details_expectation_status = metrics_details.filter(metrics_details.expectation_status.isNotNull())
            metrics_details_scanned = metrics_details.filter(metrics_details.scanned.isNotNull())
            metrics_details_success = metrics_details.filter(metrics_details.success.isNotNull())
            metrics_details_failure = metrics_details.filter(metrics_details.failure.isNotNull())
            metrics_details_details = metrics_details.filter(metrics_details.details.isNotNull())

            # Create metrics_details_final df
            metrics_details_final = metrics_details_expectation_status.drop('scanned', 'success', 'failure', 'details')

            metrics_details_final = metrics_details_final.alias('final').join(metrics_details_scanned.alias('scanned'),
                                                                              on=['expectation_name', 'run_id',
                                                                                  'schema_name', 'table_name',
                                                                                  'column_name'],
                                                                              how='left') \
                .select("final.*", "scanned.scanned")

            metrics_details_final = metrics_details_final.alias('final').join(metrics_details_success.alias('success'),
                                                                              on=['expectation_name', 'run_id',
                                                                                  'schema_name', 'table_name',
                                                                                  'column_name'],
                                                                              how='left') \
                .select("final.*", "success.success")

            metrics_details_final = metrics_details_final.alias('final').join(metrics_details_failure.alias('failure'),
                                                                              on=['expectation_name', 'run_id',
                                                                                  'schema_name', 'table_name',
                                                                                  'column_name'],
                                                                              how='left') \
                .select("final.*", "failure.failure")

            metrics_details_final = metrics_details_final.alias('final').join(metrics_details_details.alias('details'),
                                                                              on=['expectation_name', 'run_id',
                                                                                  'schema_name', 'table_name',
                                                                                  'column_name'],
                                                                              how='left') \
                .select("final.*", "details.details")

            # Drop duplicates based on expectation_name, column_name
            # Select only columns to be loaded to DDQ_JOBS_EXECUTION_DETAILS
            metrics_df = metrics_details_final.drop_duplicates(['expectation_name', 'column_name']).select('job_id',
                                                                                                           'config_id',
                                                                                                           'expectation_status',
                                                                                                           'scanned',
                                                                                                           'success',
                                                                                                           'failure',
                                                                                                           'details')

            # # Fix success column
            # # Improve this logic
            metrics_df = metrics_df.withColumn('success', (col('scanned') - col('failure')).astype(IntegerType()))

            # Write df to DDQ_JOBS_EXECUTION_DETAILS
            metrics_df.write.format("jdbc").options(
                url=f"jdbc:sqlserver://{self.db_servername}:{self.jdbcPort};database={self.db_name_control}",
                driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
                dbtable="ddq.ddq_jobs_execution_details",
                user=self.db_user,
                password=self.db_pass
            ).mode('append').save()

            return

        except Exception as e:
            print("Failed to update execution details: ", e)

    def get_failed_records_details(self, **kwargs):
        """
        Creates detailed view on failed records and loads the dataframe to a blob storage.

        Parameters
        ----------
        batchid: The Great Expectations batchid/runname/runid

        Returns
        -------
        N/A
        """
        # Create batch_id variable
        batch_id = kwargs.get('batchid')

        # Get details from SQL
        query = f"""
    SELECT 
      A.job_id
     ,A.config_id
     ,A.expectation_status
     ,B.exp_id
     ,B.schema_name
     ,B.table_name
     ,B.column_name
     ,B.expectation_parameters
     ,C.expectation_name
     ,D.job_name
     ,D.run_id
     ,D.pipeline_run_id_name
     ,D.pipeline_run_id
    FROM ddq.ddq_jobs_execution_details A
    INNER JOIN ddq.ddq_expectations_config B ON (A.config_id = B.config_id)
    INNER JOIN ddq.ddq_expectations C ON (B.exp_id = C.exp_id)
    INNER JOIN ddq.ddq_jobs_execution D ON (A.job_id = D.job_id)
    WHERE D.run_id = '{batch_id}'
    """

        exp_df = self.helperfunctions.read_table(query=query, db=self.db_name_control)

        # Filter for only failed expectations
        exp_df = exp_df.drop_duplicates(['expectation_name', 'schema_name', 'table_name', 'column_name']).filter(
            (exp_df.expectation_status == 'false'))

        if exp_df.count() > 0:
            # Create empty Spark dataframe
            # This empty df will be used to union failed results df
            original_table = self.helperfunctions.read_table(
                query=f"SELECT TOP(1) * FROM {exp_df.first()['schema_name']}.{exp_df.first()['table_name']}")
            schema = original_table.schema
            schema.add(StructField("expectation_name", StringType(), True)).add(
                StructField("parameters", StringType(), True)).add(StructField("run_id", StringType(), True))
            fail_df = self.spark.createDataFrame(data=[], schema=schema)

            for row in exp_df.collect():
                exp_params = json.loads(row.expectation_parameters)

                # Collect failed records for each expectation_name

                if row.expectation_name == 'expect_column_distinct_values_to_be_in_set':
                    query = f"""
          SELECT * 
          FROM {row.schema_name}.{row.table_name} 
          WHERE {row.column_name} NOT IN {tuple(exp_params['value_set'])}
          """
                    print(query)
                    if row.pipeline_run_id is not None:
                        query = query + f" AND [{row.pipeline_run_id_name}] = '{row.pipeline_run_id}'"
                    else:
                        pass
                    temp_df = self.helperfunctions.read_table(query=query)
                    temp_df = temp_df.withColumn('expectation_name', lit(row.expectation_name)).withColumn('parameters',
                                                                                                           lit(row.expectation_parameters)).withColumn(
                        'run_id', lit(row.run_id))
                    fail_df = fail_df.union(temp_df)

                elif row.expectation_name == 'expect_column_values_to_be_unique':
                    query = f"""
          SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY {row.column_name} ORDER BY {row.column_name}) AS Dups
            FROM {row.schema_name}.{row.table_name} ) A
          WHERE A.Dups > 1
          """
                    print(query)
                    if row.pipeline_run_id is not None:
                        query = query + f" AND [{row.pipeline_run_id_name}] = '{row.pipeline_run_id}'"
                    else:
                        pass
                    temp_df = self.helperfunctions.read_table(query=query)
                    temp_df = temp_df.drop('Dups')
                    temp_df = temp_df.withColumn('expectation_name', lit(row.expectation_name)).withColumn('parameters',
                                                                                                           lit(row.expectation_parameters)).withColumn(
                        'run_id', lit(row.run_id))
                    fail_df = fail_df.union(temp_df)

                elif row.expectation_name == 'expect_column_values_to_be_in_set':
                    query = f"""
          SELECT *
          FROM {row.schema_name}.{row.table_name}
          WHERE {row.column_name} NOT IN {tuple(exp_params['value_set'])}
          """
                    print(query)
                    if row.pipeline_run_id is not None:
                        query = query + f" AND [{row.pipeline_run_id_name}] = '{row.pipeline_run_id}'"
                    else:
                        pass
                    temp_df = self.helperfunctions.read_table(query=query)
                    temp_df = temp_df.withColumn('expectation_name', lit(row.expectation_name)).withColumn('parameters',
                                                                                                           lit(row.expectation_parameters)).withColumn(
                        'run_id', lit(row.run_id))
                    fail_df = fail_df.union(temp_df)

                elif row.expectation_name == 'expect_column_values_to_not_be_null':
                    query = f"""
          SELECT *
          FROM {row.schema_name}.{row.table_name}
          WHERE {row.column_name} IS NULL
          """
                    print(query)
                    if row.pipeline_run_id is not None:
                        query = query + f" AND [{row.pipeline_run_id_name}] = '{row.pipeline_run_id}'"
                    else:
                        pass
                    temp_df = self.helperfunctions.read_table(query=query)
                    temp_df = temp_df.withColumn('expectation_name', lit(row.expectation_name)).withColumn('parameters',
                                                                                                           lit(row.expectation_parameters)).withColumn(
                        'run_id', lit(row.run_id))
                    fail_df = fail_df.union(temp_df)

                elif row.expectation_name == 'expect_column_value_lengths_to_equal':
                    query = f"""
          SELECT *
          FROM {row.schema_name}.{row.table_name}
          WHERE LEN(CAST({row.column_name} as VARCHAR(MAX))) !=  {(exp_params['value'])}
          """
                    print(query)
                    if row.pipeline_run_id is not None:
                        query = query + f" AND [{row.pipeline_run_id_name}] = '{row.pipeline_run_id}'"
                    else:
                        pass
                    temp_df = self.helperfunctions.read_table(query=query)
                    temp_df = temp_df.withColumn('expectation_name', lit(row.expectation_name)).withColumn('parameters',
                                                                                                           lit(row.expectation_parameters)).withColumn(
                        'run_id', lit(row.run_id))
                    fail_df = fail_df.union(temp_df)

                elif row.expectation_name == 'expect_column_values_to_match_like_pattern':
                    query = f"""
          SELECT *
          FROM {row.schema_name}.{row.table_name}
          WHERE {row.column_name} Not Like '{(exp_params['like_pattern'])}'
          """
                    print(query)
                    if row.pipeline_run_id is not None:
                        query = query + f" AND [{row.pipeline_run_id_name}] = '{row.pipeline_run_id}'"
                    else:
                        pass
                    temp_df = self.helperfunctions.read_table(query=query)
                    temp_df = temp_df.withColumn('expectation_name', lit(row.expectation_name)).withColumn('parameters',
                                                                                                           lit(row.expectation_parameters)).withColumn(
                        'run_id', lit(row.run_id))
                    fail_df = fail_df.union(temp_df)

                else:
                    print("Failure framework for expectation name cannot be found: ")

            # # Create Delta files for fail_df
            table_name = exp_df.first()['table_name']
            linked_service_name = 'WorkspaceDefaultStorage'

            self.spark.conf.set("spark.storage.synapse.linkedServiceName", linked_service_name)
            self.spark.conf.set("fs.azure.account.oauth.provider.type",
                           "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider")
            fail_df.write.format('delta').mode('append').parquet(
                f'abfss://<YourContainerPath>.dfs.core.windows.net/{table_name}')


            print("Write to Delta Lake storage is complete")

            return

        else:
            print("No failed records found")

