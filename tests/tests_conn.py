import sys
sys.path.append("./workflows")
from utils.conn_utils import *

def test_pyspark_session():
    print("Testing function: pyspark_session...")

    test_args = [{"test_name": "Success",
                  "parameters": "../utils/jars/postgresql-42.7.1.jar",
                  "expected_results": {"text": "<pyspark.sql.session.SparkSession object at", "found": True}
                  },
                 ]

    for test in test_args:
        print(f"...testing for: {test['test_name']}...")

        expected_results = test['expected_results']['found']
        initial_results = pyspark_session(jars_path=test["parameters"])

        results = test["expected_results"]["text"] in str(initial_results)

        print(f"expected_results: {expected_results}")
        print(f"results: {results}")
        assert expected_results == results


def test_pyspark_read_csv():
    from pyspark.sql.types import StructType, StructField, IntegerType
    print("Testing function: pyspark_read_csv...")
    spark = pyspark_session()

    sample_data = "./tests/sources_test/sales_data_sample.csv"
    test_args = [{"test_name": "Success",
                  "parameters": {"file_path":sample_data,
                                 "inferSchema": True},
                  "expected_results": "DataFrame[transaction_id: int, sale_value: int]"
                  },
                 {"test_name": "Fail_file",
                  "parameters": {"file_path": "",
                                 "inferSchema": True},
                  "expected_results": "Can not create a Path from an empty string"
                  },
                 {"test_name": "Fail_config",
                  "parameters": {"file_path": sample_data,
                                 "inferSchema": ""},
                  "expected_results": "Error in parameters"
                  },
                 ]

    for test in test_args:
        print(f"...testing for: {test['test_name']}...")

        expected_results = test["expected_results"]
        results = pyspark_read_csv(spark=spark, file_path=test["parameters"]["file_path"],
                                   inferSchema=test["parameters"]["inferSchema"])

        print(f"expected_results: {test['expected_results']}")
        print(f"results: {results}")
        assert expected_results == str(results)
