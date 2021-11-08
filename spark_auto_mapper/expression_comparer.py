import re

from pyspark.sql.column import Column


def assert_expressions_are_equal(expr1: Column, expr2: Column) -> None:
    # Spark 3.2 randomly assigns a postfix to lambda variables so direct string comparison fails
    # Instead we remove this postfix here
    s_expr1 = str(expr1)
    s_expr2 = str(expr2)

    assert re.sub(r"x_\d+", "x", s_expr1) == re.sub(r"x_\d+", "x", s_expr2)
