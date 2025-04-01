from pyspark.sql.functions import col

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

# Dynamically generate test cases based on YAML configuration
def generate_test_cases(config):
    """Helper function to generate test cases."""
    test_cases = []
    for table_name, table_config in config["table_data"].items():
        validation_rules = config["validations"].get(table_name, [])

        # If "schema_match" exists in validations, add a test for schema
        if any(v["type"] == "schema_match" for v in validation_rules):
            test_cases.append((table_name, "schema_match"))

        # If "value_match" exists in validations, add a test for data
        if any(v["type"] == "value_match" for v in validation_rules):
            test_cases.append((table_name, "value_match"))

    return test_cases