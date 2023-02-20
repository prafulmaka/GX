from libraries import *
from validation_framework_execute import ValidationFrameworkExecute


# Main Class
if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder.config("spark.driver.extraClassPath", "C:/Users/pramaka/Desktop/GX/jars/mssql-jdbc-12.2.0.jre8.jar").getOrCreate()

    db_user = ""
    db_pass = ""
    db_pass_quote = ""
    db_name_control = ""
    db_name_data = ""
    db_servername = ""

    validation_framework = ValidationFrameworkExecute(
        db_user = db_user,
        db_pass = db_pass,
        db_pass_quote = db_pass_quote,
        db_name_control = db_name_control,
        db_name_data = db_name_data,
        db_servername = db_servername,
        spark = spark
    )

    validation_framework.run_expectation_suite()