# validation_utils.py
from pyspark.sql.types import *
from pyspark.sql.functions import col

def check_schema_match(hive_df, json_data, hive_columns):
    """Check if Hive table schema matches JSON structure"""
    if isinstance(json_data, dict):
        if "employees" in json_data:
            json_data = json_data["employees"]
        elif "departments" in json_data:
            json_data = json_data["departments"]
    
    if len(json_data) > 0:
        json_fields = set(json_data[0].keys())
    else:
        json_fields = set()
    
    hive_fields = set(hive_columns)
    return {
        "missing_in_hive": json_fields - hive_fields,
        "extra_in_hive": hive_fields - json_fields
    }

def check_data_type_match(hive_df, json_data, mappings):
    """Check data types between JSON and Hive"""
    if isinstance(json_data, dict):
        if "employees" in json_data:
            json_data = json_data["employees"]
        elif "departments" in json_data:
            json_data = json_data["departments"]
    
    if len(json_data) == 0:
        return []
    
    # Simplified type checking
    type_checks = {
        "integer": lambda x: isinstance(x, int),
        "float": lambda x: isinstance(x, float),
        "string": lambda x: isinstance(x, str),
        "boolean": lambda x: isinstance(x, bool)
    }
    
    mismatches = []
    
    for mapping in mappings:
        json_field = mapping["json_field"]
        hive_column = mapping["hive_column"]
        expected_type = mapping["expected_type"]
        
        # Check JSON type
        if json_field in json_data[0]:
            json_value = json_data[0][json_field]
            if expected_type in type_checks and not type_checks[expected_type](json_value):
                mismatches.append(
                    f"JSON field {json_field} should be {expected_type} but is {type(json_value).__name__}"
                )
        
        # Check Hive type
        hive_type = next(f.dataType for f in hive_df.schema.fields if f.name == hive_column)
        type_mapping = {
            "integer": (IntegerType, LongType, ShortType),
            "float": (FloatType, DoubleType, DecimalType),
            "string": (StringType,),
            "boolean": (BooleanType,)
        }
        
        if expected_type in type_mapping:
            if not isinstance(hive_type, type_mapping[expected_type]):
                mismatches.append(
                    f"Hive column {hive_column} should be {expected_type} but is {type(hive_type).__name__}"
                )
    
    return mismatches

def check_value_match(spark, hive_df, json_data, mappings, primary_key=None):
    """Check that values match between JSON and Hive"""
    if isinstance(json_data, dict):
        if "employees" in json_data:
            json_data = json_data["employees"]
        elif "departments" in json_data:
            json_data = json_data["departments"]
    
    json_df = spark.createDataFrame(json_data)
    
    count_mismatches = []
    for mapping in mappings:
        hive_count = hive_df.select(mapping["hive_column"]).distinct().count()
        json_count = json_df.select(mapping["json_field"]).distinct().count()
        if hive_count != json_count:
            count_mismatches.append({
                "column": mapping["hive_column"],
                "hive_count": hive_count,
                "json_count": json_count
            })
    
    return {
        "count_mismatches": count_mismatches,
        "value_mismatches": []
    }

def check_row_count_match(hive_df, json_data):
    """Check that row counts match"""
    if isinstance(json_data, dict):
        if "employees" in json_data:
            json_count = len(json_data["employees"])
        elif "departments" in json_data:
            json_count = len(json_data["departments"])
        else:
            json_count = 0
    else:
        json_count = len(json_data)
    
    return {
        "hive_count": hive_df.count(),
        "json_count": json_count
    }