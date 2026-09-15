import re

# noinspection PyPackageRequirements
from pyspark.sql.column import Column


def fix_generated_lambda_variable_names(
    expression_text: str, ignore_casts: bool = True
) -> str:
    """
    Spark generates "random" names for lambda variables which makes it hard to compare
    So we replace the postfixes.

    Also normalizes Spark 4.x vs 3.x Column string representation differences:
    - Spark 4.x quotes string/numeric literals with single quotes in Column.__repr__
    - Spark 4.x adds a space after CAST keyword: "CAST (" vs "CAST("


    :param expression_text: text of expression
    :param ignore_casts: whether to ignore cast operations

    :returns cleaned expression text
    """
    replace_lambda_variables = re.sub(r"x_(\d+)", "x", expression_text)
    # Normalize Spark 4.x quoting: remove single quotes around identifiers/literals
    # in Column repr (e.g., 'usual' -> usual, '100' -> 100)
    normalized = re.sub(r"'([^']*)'", r"\1", replace_lambda_variables)
    # Normalize "CAST (" -> "CAST(" (Spark 4.x adds a space)
    normalized = normalized.replace("CAST (", "CAST(")
    replace_casts = (
        re.sub(r"CAST\((.*)\s\w*\s\w*\)", r"\1", normalized)
        if ignore_casts
        else normalized
    )
    return replace_casts


def assert_compare_expressions(
    expression1: Column, expression2: Column, ignore_casts: bool = True
) -> None:
    """
    Asserts whether the two Spark expressions are the same


    :param expression1: expression 1
    :param expression2: expression 2
    :param ignore_casts: whether to ignore cast operations
    """
    expression_text1: str = fix_generated_lambda_variable_names(
        str(expression1), ignore_casts=ignore_casts
    )
    expression_text2: str = fix_generated_lambda_variable_names(
        str(expression2), ignore_casts=ignore_casts
    )
    assert (
        expression_text1 == expression_text2
    ), f"{expression_text1} did not match {expression_text2}"
