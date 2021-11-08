import re

from pyspark.sql.column import Column


def compare_expressions(expr1: Column, expr2: Column) -> bool:
    # Spark 3.2 randomly assigns a postfix to lambda variables so direct string comparison fails
    # Instead we remove this postfix here
    s_expr1 = str(expr1)
    s_expr2 = str(expr2)

    comparison = re.sub(r'x_\d', 'x', s_expr1) == re.sub(r'x_\d', 'x', s_expr2)
    return comparison
