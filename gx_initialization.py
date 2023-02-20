# Import dependencies
from libraries import *

class ValidationFrameworkInit:
    """
    This class initializes and contains all methods for the Data Validation framework using Great Expectations.
    """

    def __init__(self, **kwargs):
        """
        GX initialization using DataContextConfig.

        Parameters
        ----------
        db: The database name of the datasource to be used (Optional)

        Returns
        -------
        N/A
        """
        try:
            # Set default variable
            self.db_user = kwargs.get('db_user')
            self.db_pass = kwargs.get('db_pass')
            self.db_pass_quote = kwargs.get('db_pass_quote')
            self.db_name_control = kwargs.get('db_name_control')
            self.db_name_data = kwargs.get('db_name_data')
            self.db_servername = kwargs.get('db_servername')
            self.jdbcPort = kwargs.get('jdbcPort')

            # Set db_name variable if kwarg exists
            if 'db' in kwargs:
                self.db_name_data = kwargs.get('db')

            # Initialize Great Expectations
            # Setup datasource using Active Directory authentication
            self.data_context_config = DataContextConfig(
                config_version=3.0,
                plugins_directory=None,
                config_variables_file_path=None,
                datasources={
                    "my_spark_datasource_config": DatasourceConfig(
                        class_name="Datasource",
                        execution_engine={
                            "class_name": "SqlAlchemyExecutionEngine",
                            "module_name": "great_expectations.execution_engine",
                            "connection_string": f"mssql+pyodbc://{self.db_user}:{self.db_pass}@{self.db_servername}/{self.db_name_data}?driver=ODBC Driver 17 for SQL Server"},
                        data_connectors={
                            "default_runtime_data_connector_name": {
                                "class_name": "RuntimeDataConnector",
                                "module_name": "great_expectations.datasource.data_connector",
                                "batch_identifiers": ["batchid"]
                            },
                            "default_inferred_data_connector_name": {
                                "class_name": "InferredAssetSqlDataConnector",
                                "module_name": "great_expectations.datasource.data_connector",
                                "introspection_directives": {"schema_name": "dbo"},
                                "include_schema_name": "true"
                            },
                            "default_configured_data_connector_name": {
                                "class_name": "ConfiguredAssetSqlDataConnector",
                                "module_name": "great_expectations.datasource.data_connector",
                                "assets": {"GXtest": {"class_name": "Asset",
                                                      "module_name": "great_expectations.datasource.data_connector.asset",
                                                      "schema_name": "dbo"}}
                            }
                        }
                    )
                },
                stores={
                    "expectations_SQL_store": {
                        "class_name": "ExpectationsStore",
                        "store_backend": {
                            "class_name": "DatabaseStoreBackend",
                            "connection_string": f"mssql+pyodbc://{self.db_user}:{self.db_pass}@{self.db_servername}/{self.db_name_control}?driver=ODBC Driver 17 for SQL Server"
                        },
                    },
                    "validations_SQL_store": {
                        "class_name": "ValidationsStore",
                        "store_backend": {
                            "class_name": "DatabaseStoreBackend",
                            "connection_string": f"mssql+pyodbc://{self.db_user}:{self.db_pass}@{self.db_servername}/{self.db_name_control}?driver=ODBC Driver 17 for SQL Server"
                        },
                    },
                    "metric_store": {
                        "class_name": "MetricStore",
                        "store_backend": {
                            "class_name": "DatabaseStoreBackend",
                            "connection_string": f"mssql+pyodbc://{self.db_user}:{self.db_pass}@{self.db_servername}/{self.db_name_control}?driver=ODBC Driver 17 for SQL Server"
                        }
                    },
                    "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
                },
                expectations_store_name="expectations_SQL_store",
                validations_store_name="validations_SQL_store",
                evaluation_parameter_store_name="evaluation_parameter_store",
                validation_operators={
                    "action_list_operator": {
                        "class_name": "ActionListValidationOperator",
                        "action_list": [
                            {
                                "name": "store_validation_result",
                                "action": {"class_name": "StoreValidationResultAction"},
                            },
                            {
                                "name": "store_evaluation_params",
                                "action": {"class_name": "StoreEvaluationParametersAction"},
                            },
                            {
                                "name": "update_data_docs",
                                "action": {"class_name": "UpdateDataDocsAction"},
                            },
                            {
                                "name": "store_metrics",
                                "action": {"class_name": "StoreMetricsAction",
                                           "target_store_name": "metric_store",
                                           "requested_metrics": "*"}
                            }
                        ],
                    }
                },
                #       data_docs_sites={
                #           "local_site": {
                #               "class_name": "SiteBuilder",
                #               "store_backend": {
                #               "class_name": "TupleAzureBlobStoreBackend",
                #               "container": "ge-dataquality",
                #               "connection_string": "<Your Bucket String>"
                #           },
                #           "site_index_builder": {
                #               "class_name": "DefaultSiteIndexBuilder",
                #               "show_cta_footer": True
                #           }
                #       }
                #     },
                anonymous_usage_statistics={
                    "enabled": False
                }
            )
        except Exception as e:
            print('Failed in initialization')