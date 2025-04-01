from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import yaml

# Initialize Spark Session with Hive support
spark = SparkSession.builder \
    .appName("DataValidationTest") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a Hive database and table for employees if they don't exist
spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

spark.sql("DROP TABLE IF EXISTS test_db.employees")

spark.sql("""
CREATE TABLE IF NOT EXISTS test_db.employees (
    emp_id INT,
    employee_name STRING,
    work_email STRING,
    dept_name STRING,
    annual_salary DOUBLE,
    company_name STRING,
    joining_date STRING
) USING hive
""")

# Insert sample data into the Hive table (you can truncate before insert in a real test environment)
spark.sql("""
INSERT INTO test_db.employees VALUES
(101, 'John Doe', 'johndoe@gmail.com', 'Engineering', 75000.50, 'ABC Inc', '2020-01-15'),
(102, 'Jane Smith', 'janesmith@gmail.com', 'Marketing', 65000.00, 'XYZ Inc', '2019-11-15'),
(103, 'Tom Brown', 'tombrown@gmail.com', 'Sales', 55000.00, 'LMN Inc', '2021-03-10')
""")

# Load config.yaml
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Load input JSON for employees
with open(config['inputs']['employees']['input_file_path'], "r") as f:
    employees_data = json.load(f)

# Convert JSON data to PySpark DataFrame
df = spark.createDataFrame(employees_data["employees"])
print(df.columns)

def apply_transformations(df, mappings):
    """
    Apply transformations dynamically based on the mappings in the config.yaml.
    """
    drop_columns = []
    for field, mapping in mappings.items():
        source_field = mapping['source']
        target_field = mapping['target']
        source_type = mapping['sourceType']
        target_type = mapping['targetType']
        
        # Handle nested fields using dictionary-style access
        
        if '.' in source_field:
            nested_fields = source_field.split('.')
            nested_col = col(nested_fields[0])
            for field in nested_fields[1:]:
                nested_col = nested_col.getItem(field)
            df = df.withColumn(target_field, nested_col)
            drop_columns.append(nested_fields[0])
        else:
            df = df.withColumnRenamed(source_field, target_field)
        
        # Drop the source column if needed
        # Apply type casting if needed
        if source_type != target_type:
            df = df.withColumn(target_field, col(target_field).cast(target_type))
    
    df = df.drop(*drop_columns)
    return df

# Apply transformations for employees using mappings from config.yaml
employee_mappings = config['mappings']['employees']
transformed_df = apply_transformations(df, employee_mappings)

# Expected Output for employees
expected_df = spark.sql("SELECT * FROM test_db.employees")

# # PyTest Test Cases
def test_schema():
    """Check if schema of transformed dataframe matches the expected schema."""
    # Convert both schemas to sorted lists of StructFields for comparison
    actual_schema = sorted(transformed_df.schema.fields, key=lambda x: x.name)
    expected_schema = sorted(expected_df.schema.fields, key=lambda x: x.name)

    # Compare the sorted schema fields
    assert actual_schema == expected_schema, f"Schema mismatch! {actual_schema} != {expected_schema}"

def test_data():
    """Check if data in transformed dataframe matches the expected output, ignoring row order and column order."""
    # Reorder columns in transformed_df to match the expected order
    expected_columns = expected_df.columns
    transformed_df_ordered = transformed_df.select(*expected_columns)

    # Compare row-wise after reordering columns
    actual_data = set(transformed_df_ordered.collect())
    expected_data = set(expected_df.collect())

    assert actual_data == expected_data, f"Data mismatch!\nActual: {actual_data}\nExpected: {expected_data}"

