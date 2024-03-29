import pytest

import py_modules.word_count

# this allows using the fixture in all tests in this module
pytestmark = pytest.mark.usefixtures("spark_context")


def test_do_word_counts(spark_context):

    test_input = [
        ' hello spark ',
        ' hello again spark spark'
    ]

    input_rdd = spark_context.parallelize(test_input, 1)
    results = py_modules.word_count.do_word_counts(input_rdd)

    expected_results = {'hello': 2, 'spark': 3, 'again': 2}
    assert results == expected_results
