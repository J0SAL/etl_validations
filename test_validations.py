# test_validations.py
import pytest
import json
import yaml
from validation_utils import *

def get_test_cases():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    
    test_cases = []
    for validation in config["validations"]:
        for test in validation["tests"]:
            test_cases.append({
                "table": validation["table"],
                "test_type": test["type"],
                "json_path": validation["json_path"],
                "test_config": test,
                "table_config": validation
            })
    return test_cases

@pytest.mark.parametrize("test_case", get_test_cases())
def test_validations(test_case, spark):
    # Load Hive data
    hive_df = spark.sql(f"SELECT * FROM {test_case['table']}")
    
    # Load JSON data
    with open(test_case["json_path"]) as f:
        json_data = json.load(f)
    
    # Run validation
    test_type = test_case["test_config"]["type"]
    
    if test_type == "schema_match":
        result = check_schema_match(
            hive_df,
            json_data,
            test_case["test_config"]["hive_columns"]
        )
        assert not result["missing_in_hive"], f"Missing columns: {result['missing_in_hive']}"
        assert not result["extra_in_hive"], f"Extra columns: {result['extra_in_hive']}"
    
    elif test_type == "data_type_match":
        mismatches = check_data_type_match(
            hive_df,
            json_data,
            test_case["test_config"]["mappings"]
        )
        assert not mismatches, "\n".join(mismatches)
    
    elif test_type == "value_match":
        result = check_value_match(
            spark,
            hive_df,
            json_data,
            test_case["test_config"]["mappings"],
            test_case["table_config"].get("primary_key")
        )
        assert not result["count_mismatches"], "Value count mismatches found"
    
    elif test_type == "row_count_match":
        result = check_row_count_match(hive_df, json_data)
        assert result["hive_count"] == result["json_count"], \
            f"Row count mismatch: Hive={result['hive_count']}, JSON={result['json_count']}"