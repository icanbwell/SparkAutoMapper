import re

# noinspection PyPackageRequirements
from pyspark.sql.column import Column


def fix_generated_lambda_variable_names(expression_text: str) -> str:
    """
    Spark generates "random" names for lambda variables which makes it hard to compare
    So we replace the postfixes


    :param expression_text: text of expression
    :returns cleaned expression text
    """
    replace_lambda_variables = re.sub(r"x_([0-9]+)", "x", expression_text)
    replace_casts = re.sub(r"CAST\((.*)\s\w*\s\w*\)", r"\1", replace_lambda_variables)
    return replace_casts


def assert_compare_expressions(expression1: Column, expression2: Column) -> None:
    """
    Asserts whether the two Spark expressions are the same


    :param expression1: expression 1
    :param expression2: expression 2
    """
    expression_text1: str = fix_generated_lambda_variable_names(str(expression1))
    expression_text2: str = fix_generated_lambda_variable_names(str(expression2))
    assert expression_text1 == expression_text2
