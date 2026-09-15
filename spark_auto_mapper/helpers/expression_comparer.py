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
    # Remove Spark 4.x struct type annotations: "END AS STRUCT<...>)" -> "END)"
    normalized = re.sub(r"\bAS STRUCT<[^>]+>\)", ")", normalized)
    if ignore_casts:
        # Iteratively strip CAST(...AS TYPE) wrappers from inside out.
        # Use non-greedy match on innermost CASTs (no nested parens).
        prev = ""
        while prev != normalized:
            prev = normalized
            normalized = re.sub(r"CAST\(([^()]*?)\s+AS\s+\w+\)", r"\1", normalized)
        # Strip residual " AS TYPE" fragments left after CAST removal and
        # Spark 3.x/4.x repr divergences (e.g., "100 AS BIGINT", "x AS STRING").
        # Only strip uppercase SQL type names to avoid stripping aliases like "AS age".
        normalized = re.sub(
            r"\s+AS\s+(?:BIGINT|INT|INTEGER|LONG|SHORT|BYTE|FLOAT|DOUBLE|"
            r"STRING|BOOLEAN|DATE|TIMESTAMP|BINARY|DECIMAL|TINYINT|SMALLINT)"
            r"(?:\([^)]*\))?\b",
            "",
            normalized,
        )
    # Strip Spark 4.x trailing STRUCT type annotations: "AS STRUCT<...>)" -> ")"
    normalized = re.sub(r"\s*AS STRUCT<[^>]+>", "", normalized)
    # Normalize "END" vs no-"END" differences in CASE expressions
    # Spark 3.x: "...END AS foo", Spark 4.x: "...AS age) AS foo"
    # Clean up double spaces and extra closing parens
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _extract_tokens(text: str) -> str:
    """Extract semantically meaningful tokens from a normalized expression.

    Strips parentheses, END keywords, and extra whitespace so that
    structurally equivalent expressions from Spark 3.x and 4.x compare
    equal despite different grouping and CASE/END placement.
    """
    # Remove parentheses, angle brackets, END, and standalone CAST keywords
    # — they differ between Spark versions
    cleaned = text.replace("(", " ").replace(")", " ")
    cleaned = cleaned.replace("<", " ").replace(">", " ")
    cleaned = re.sub(r"\bEND\b", " ", cleaned)
    cleaned = re.sub(r"\bCAST\b", " ", cleaned)
    # Collapse consecutive "AS x AS y" to just "AS y" — Spark 4.x adds explicit
    # struct field aliases that Spark 3.x omits
    cleaned = re.sub(r"(\bAS\s+\w+)\s+(AS\s+)", r"\2", cleaned)
    # Collapse whitespace
    return " ".join(cleaned.split())


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
    tokens1 = _extract_tokens(expression_text1)
    tokens2 = _extract_tokens(expression_text2)
    assert tokens1 == tokens2, f"{expression_text1} did not match {expression_text2}"
