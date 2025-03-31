# conftest.py
import pytest
from pyspark.sql import SparkSession
import yaml

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("testing") \
        .master("local[2]") \
        .enableHiveSupport() \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    # Create test tables with EXACT schema matching JSON
    spark.sql("""
    CREATE TABLE IF NOT EXISTS employees (
        employee_id INT,
        name STRING,
        department STRING,
        salary FLOAT,
        start_date STRING
    ) USING hive
    """)
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS departments (
        dept_id INT,
        name STRING,
        manager STRING,
        budget INT
    ) USING hive
    """)
    
    # Clear and reload test data
    spark.sql("TRUNCATE TABLE employees")
    spark.sql("TRUNCATE TABLE departments")
    
    spark.sql("""
    INSERT INTO employees VALUES
        (101, 'John Doe', 'Engineering', 75000.50, '2020-01-15'),
        (102, 'Jane Smith', 'Marketing', 68000.00, '2019-05-22'),
        (103, 'Mike Johnson', 'Engineering', 82000.75, '2021-03-10')
    """)
    
    spark.sql("""
    INSERT INTO departments VALUES
        (1, 'Engineering', 'John Doe', 500000),
        (2, 'Marketing', 'Jane Smith', 300000)
    """)
    
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def config():
    with open("config.yaml") as f:
        return yaml.safe_load(f)