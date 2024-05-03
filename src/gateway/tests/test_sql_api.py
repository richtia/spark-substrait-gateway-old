# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
from pathlib import Path

import pytest
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.testing import assertDataFrameEqual

test_case_directory = Path(__file__).resolve().parent / 'data'

sql_test_case_paths = [f for f in sorted(test_case_directory.iterdir()) if f.suffix == '.sql']

sql_test_case_names = [p.stem for p in sql_test_case_paths]


@pytest.fixture(autouse=True)
def mark_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue('source')
    originalname = request.keywords.node.originalname
    if source == 'gateway-over-duckdb' and originalname == 'test_tpch':
        path = request.getfixturevalue('path')
        if path.stem in ['02', '04', '16', '17', '18', '20', '21', '22']:
            request.node.add_marker(pytest.mark.xfail(reason='DuckDB needs Delim join'))
        if path.stem in ['15']:
            request.node.add_marker(pytest.mark.xfail(reason='Rounding inconsistency'))
    if source == 'gateway-over-datafusion':
        pytest.importorskip("datafusion.substrait")
        if originalname == 'test_count':
            request.node.add_marker(pytest.mark.xfail(reason='COUNT() not implemented'))
        if originalname in ['test_tpch']:
            path = request.getfixturevalue('path')
            if path.stem in ['01']:
                request.node.add_marker(pytest.mark.xfail(reason='COUNT() not implemented'))
            elif path.stem in ['07']:
                request.node.add_marker(pytest.mark.xfail(reason='Projection uniqueness error'))
            elif path.stem in ['08']:
                request.node.add_marker(pytest.mark.xfail(reason='aggregation error'))
            elif path.stem in ['09']:
                request.node.add_marker(pytest.mark.xfail(reason='instr not implemented'))
            elif path.stem in ['11']:
                request.node.add_marker(pytest.mark.xfail(reason='first not implemented'))
            elif path.stem in ['13']:
                request.node.add_marker(pytest.mark.xfail(reason='not rlike not implemented'))
            elif path.stem in ['15']:
                request.node.add_marker(pytest.mark.xfail(reason='empty table error'))
            elif path.stem in ['16']:
                request.node.add_marker(pytest.mark.xfail(reason='mark join not implemented'))
            elif path.stem in ['18']:
                request.node.add_marker(pytest.mark.xfail(reason='out of bounds error'))
            elif path.stem in ['19']:
                request.node.add_marker(pytest.mark.xfail(reason='multiargument OR not supported'))
            elif path.stem in ['02', '04', '17', '20', '21', '22']:
                request.node.add_marker(pytest.mark.xfail(reason='DataFusion needs Delim join'))
            request.node.add_marker(pytest.mark.xfail(reason='Gateway internal iterating error'))


# pylint: disable=missing-function-docstring
# ruff: noqa: E712
class TestSqlAPI:
    """Tests of the SQL side of SparkConnect."""

    def test_count(self, spark_session_with_tpch_dataset):
        outcome = spark_session_with_tpch_dataset.sql(
            'SELECT COUNT(*) FROM customer').collect()
        assert_that(outcome[0][0], equal_to(149999))

    def test_limit(self, spark_session_with_tpch_dataset):
        expected = [
            Row(c_custkey=131074, c_phone='20-431-513-7803', c_mktsegment='AUTOMOBILE'),
            Row(c_custkey=131075, c_phone='31-589-697-1004', c_mktsegment='HOUSEHOLD'),
            Row(c_custkey=131076, c_phone='17-829-175-3577', c_mktsegment='AUTOMOBILE'),
            Row(c_custkey=131077, c_phone='31-306-822-6114', c_mktsegment='HOUSEHOLD'),
            Row(c_custkey=131078, c_phone='22-818-651-4724', c_mktsegment='FURNITURE')
        ]
        outcome = spark_session_with_tpch_dataset.sql(
            'SELECT c_custkey, c_phone, c_mktsegment FROM customer LIMIT 5').collect()
        assertDataFrameEqual(outcome, expected)

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize(
        'path',
        sql_test_case_paths,
        ids=sql_test_case_names,
    )
    def test_tpch(self, spark_session_with_tpch_dataset, path):
        """Test the TPC-H queries."""
        # Read the SQL to run.
        with open(path, "rb") as file:
            sql_bytes = file.read()
        sql = sql_bytes.decode('utf-8')
        spark_session_with_tpch_dataset.sql(sql).collect()
