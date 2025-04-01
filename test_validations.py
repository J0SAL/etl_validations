import pytest
from pyspark.sql import SparkSession
import json
import yaml
from mock_data import create_mock_data
from utils import apply_transformations, generate_test_cases

@pytest.fixture(scope="session")
def spark():
    """Initialize Spark session."""
    spark = SparkSession.builder \
        .appName("DataValidationTest") \
        .enableHiveSupport() \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="module")
def config():
    """Load the YAML config file."""
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

@pytest.fixture
def transformed_df(spark, table_name, config):
    """Create a transformed DataFrame for the given table."""
    create_mock_data(spark)

    # Load input data dynamically based on table name
    input_file = config["table_data"][table_name]["input_file_path"]
    with open(input_file, "r") as f:
        data = json.load(f)

    # Convert JSON data to PySpark DataFrame
    source_df = spark.createDataFrame(data[table_name])

    # Apply transformations using mappings from config.yaml
    mappings = config["mappings"][table_name]
    return apply_transformations(source_df, mappings)

@pytest.fixture
def expected_df(spark, table_name, config):
    """Load expected DataFrame from the Hive table."""
    hive_db = config["table_data"][table_name]["hive_database"]
    hive_table = config["table_data"][table_name]["hive_table"]
    return spark.sql(f"SELECT * FROM {hive_db}.{hive_table}")

# 🧪 Dynamically generate test cases using the helper
config_data = yaml.safe_load(open("config.yaml", "r"))
test_cases = generate_test_cases(config_data)

@pytest.mark.parametrize("table_name, validation_type", test_cases)
def test_table_validation(table_name, validation_type, transformed_df, expected_df):
    """Test schema and data validation for each table."""
    if validation_type == "schema_match":
        actual_schema = sorted(transformed_df.schema.fields, key=lambda x: x.name)
        expected_schema = sorted(expected_df.schema.fields, key=lambda x: x.name)
        assert actual_schema == expected_schema, f"Schema mismatch for {table_name}!"

    elif validation_type == "value_match":
        expected_columns = expected_df.columns
        transformed_df_ordered = transformed_df.select(*expected_columns)

        actual_data = set(transformed_df_ordered.collect())
        expected_data = set(expected_df.collect())

        assert actual_data == expected_data, f"Data mismatch for {table_name}!"
