

def create_mock_data(spark):
    # Create a Hive database and table for employees if they don't exist
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")

    spark.sql("DROP TABLE IF EXISTS test_db.employees")
    spark.sql("DROP TABLE IF EXISTS test_db.departments")

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

    spark.sql("""
    CREATE TABLE IF NOT EXISTS test_db.departments (
        dept_id INT,
        dept_name STRING
    ) USING hive
    """)
    

    # Insert sample data into the Hive table (you can truncate before insert in a real test environment)
    spark.sql("""
    INSERT INTO test_db.employees VALUES
    (101, 'John Doe', 'johndoe@gmail.com', 'Engineering', 75000.50, 'ABC Inc', '2020-01-15'),
    (102, 'Jane Smith', 'janesmith@gmail.com', 'Marketing', 65000.00, 'XYZ Inc', '2019-11-15'),
    (103, 'Tom Brown', 'tombrown@gmail.com', 'Sales', 55000.00, 'LMN Inc', '2021-03-10')
    """)

    spark.sql("""
    INSERT INTO test_db.departments VALUES
    (1, 'Engineering'),
    (2, 'Marketing')
    """)