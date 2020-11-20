# These are wrappers for higher order functions in Spark
# They are available in core Spark but the wrappers were only added in Spark 3.1.0 which is not available yet
#   They were added in PR: https://github.com/apache/spark/pull/27406/files
# So we have copied these wrapper functions here for now until Spark 3.1.0 becomes available

import sys
from typing import Any, List, Union

# noinspection PyProtectedMember
from pyspark import since, SparkContext
# noinspection PyProtectedMember
from pyspark.sql.column import Column, _to_java_column, _to_seq


# noinspection PyProtectedMember
def _unresolved_named_lambda_variable(*name_parts: Any) -> Column:
    """
    Create `o.a.s.sql.expressions.UnresolvedNamedLambdaVariable`,
    convert it to o.s.sql.Column and wrap in Python `Column`
    :param name_parts: str
    """
    sc = SparkContext._active_spark_context
    name_parts_seq = _to_seq(sc, name_parts)
    expressions = sc._jvm.org.apache.spark.sql.catalyst.expressions
    return Column(
        sc._jvm.Column(
            expressions.UnresolvedNamedLambdaVariable(name_parts_seq)
        )
    )


# noinspection SpellCheckingInspection
def _get_lambda_parameters(f: Any) -> Any:
    import inspect

    signature = inspect.signature(f)
    parameters = signature.parameters.values()

    # We should exclude functions that use
    # variable args and keyword argnames
    # as well as keyword only args
    supported_parameter_types = {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    }

    # Validate that
    # function arity is between 1 and 3
    if not (1 <= len(parameters) <= 3):
        raise ValueError(
            "f should take between 1 and 3 arguments, but provided function takes {}"
            .format(len(parameters))
        )

    # and all arguments can be used as positional
    if not all(p.kind in supported_parameter_types for p in parameters):
        raise ValueError(
            "f should use only POSITIONAL or POSITIONAL OR KEYWORD arguments"
        )

    return parameters


# noinspection PyDeprecation
def _get_lambda_parameters_legacy(f: Any) -> Any:
    # TODO (SPARK-29909) Remove once 2.7 support is dropped
    import inspect

    spec = inspect.getargspec(f)
    if not 1 <= len(spec.args) <= 3 or spec.varargs or spec.keywords:
        raise ValueError(
            "f should take between 1 and 3 arguments, but provided function takes {}"
            .format(spec)
        )
    return spec.args


# noinspection PyProtectedMember,SpellCheckingInspection
def _create_lambda(f: Any) -> Any:
    """
    Create `o.a.s.sql.expressions.LambdaFunction` corresponding
    to transformation described by f
    :param f: A Python of one of the following forms:
            - (Column) -> Column: ...
            - (Column, Column) -> Column: ...
            - (Column, Column, Column) -> Column: ...
    """
    if sys.version_info >= (3, 3):
        parameters = _get_lambda_parameters(f)
    else:
        parameters = _get_lambda_parameters_legacy(f)

    sc = SparkContext._active_spark_context
    expressions = sc._jvm.org.apache.spark.sql.catalyst.expressions

    argnames = ["x", "y", "z"]
    args = [
        _unresolved_named_lambda_variable(arg)
        for arg in argnames[:len(parameters)]
    ]

    result = f(*args)

    if not isinstance(result, Column):
        raise ValueError("f should return Column, got {}".format(type(result)))

    jexpr = result._jc.expr()
    jargs = _to_seq(sc, [arg._jc.expr() for arg in args])

    return expressions.LambdaFunction(jexpr, jargs, False)


# noinspection PyProtectedMember,SpellCheckingInspection
def _invoke_higher_order_function(
    name: str, cols: List[Union[str, Column]], funs: Any
) -> Column:
    """
    Invokes expression identified by name,
    (relative to ```org.apache.spark.sql.catalyst.expressions``)
    and wraps the result with Column (first Scala one, then Python).
    :param name: Name of the expression
    :param cols: a list of columns
    :param funs: a list of((*Column) -> Column functions.
    :return: a Column
    """
    sc = SparkContext._active_spark_context
    expressions = sc._jvm.org.apache.spark.sql.catalyst.expressions
    expr = getattr(expressions, name)

    jcols = [_to_java_column(col).expr() for col in cols]
    jfuns = [_create_lambda(f) for f in funs]

    return Column(sc._jvm.Column(expr(*jcols + jfuns)))


# noinspection PyUnresolvedReferences
@since(3.1)
def transform(col: Union[str, Column], f: Any) -> Column:
    """
    Returns an array of elements after applying a transformation to each element in the input array.
    :param col: name of column or expression
    :param f: a function that is applied to each element of the input array.
        Can take one of the following forms:
        - Unary ``(x: Column) -> Column: ...``
        - Binary ``(x: Column, i: Column) -> Column...``, where the second argument is
            a 0-based index of the element.
        and can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([(1, [1, 2, 3, 4])], ("key", "values"))
    >>> df.select(transform("values", lambda x: x * 2).alias("doubled")).show()
    +------------+
    |     doubled|
    +------------+
    |[2, 4, 6, 8]|
    +------------+
    >>> def alternate(x, i):
    ...     return when(i % 2 == 0, x).otherwise(-x)
    >>> df.select(transform("values", alternate).alias("alternated")).show()
    +--------------+
    |    alternated|
    +--------------+
    |[1, -2, 3, -4]|
    +--------------+
    """
    return _invoke_higher_order_function("ArrayTransform", [col], [f])


# noinspection PyUnresolvedReferences
@since(3.1)
def exists(col: Union[str, Column], f: Any) -> Column:
    """
    Returns whether a predicate holds for one or more elements in the array.
    :param col: name of column or expression
    :param f: an function ``(x: Column) -> Column: ...``  returning the Boolean expression.
        Can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([(1, [1, 2, 3, 4]), (2, [3, -1, 0])],("key", "values"))
    >>> df.select(exists("values", lambda x: x < 0).alias("any_negative")).show()
    +------------+
    |any_negative|
    +------------+
    |       false|
    |        true|
    +------------+
    """
    return _invoke_higher_order_function("ArrayExists", [col], [f])


# noinspection SpellCheckingInspection,PyUnresolvedReferences
@since(3.1)
def forall(col: Union[str, Column], f: Any) -> Column:
    """
    Returns whether a predicate holds for every element in the array.
    :param col: name of column or expression
    :param f: an function ``(x: Column) -> Column: ...``  returning the Boolean expression.
        Can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame(
    ...     [(1, ["bar"]), (2, ["foo", "bar"]), (3, ["foobar", "foo"])],
    ...     ("key", "values")
    ... )
    >>> df.select(forall("values", lambda x: x.rlike("foo")).alias("all_foo")).show()
    +-------+
    |all_foo|
    +-------+
    |  false|
    |  false|
    |   true|
    +-------+
    """
    return _invoke_higher_order_function("ArrayForAll", [col], [f])


# noinspection PyShadowingBuiltins,PyUnresolvedReferences
@since(3.1)
def filter(col: Union[str, Column], f: Any) -> Column:
    """
    Returns an array of elements for which a predicate holds in a given array.
    :param col: name of column or expression
    :param f: A function that returns the Boolean expression.
        Can take one of the following forms:
        - Unary ``(x: Column) -> Column: ...``
        - Binary ``(x: Column, i: Column) -> Column...``, where the second argument is
            a 0-based index of the element.
        and can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame(
    ...     [(1, ["2018-09-20",  "2019-02-03", "2019-07-01", "2020-06-01"])],
    ...     ("key", "values")
    ... )
    >>> def after_second_quarter(x):
    ...     return month(to_date(x)) > 6
    >>> df.select(
    ...     filter("values", after_second_quarter).alias("after_second_quarter")
    ... ).show(truncate=False)
    +------------------------+
    |after_second_quarter    |
    +------------------------+
    |[2018-09-20, 2019-07-01]|
    +------------------------+
    """
    return _invoke_higher_order_function("ArrayFilter", [col], [f])


# noinspection PyUnresolvedReferences,PyShadowingNames,PyShadowingBuiltins
@since(3.1)
def aggregate(
    col: Union[str, Column],
    zero: Union[str, Column],
    merge: Any,
    finish: Any = None
) -> Column:
    """
    Applies a binary operator to an initial state and all elements in the array,
    and reduces this to a single state. The final state is converted into the final result
    by applying a finish function.
    Both functions can use methods of :class:`pyspark.sql.Column`, functions defined in
    :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
    Python ``UserDefinedFunctions`` are not supported
    (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :param col: name of column or expression
    :param zero: initial value. Name of column or expression
    :param merge: a binary function ``(acc: Column, x: Column) -> Column...`` returning expression
        of the same type as ``zero``
    :param finish: an optional unary function ``(x: Column) -> Column: ...``
        used to convert accumulated value.
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([(1, [20.0, 4.0, 2.0, 6.0, 10.0])], ("id", "values"))
    >>> df.select(aggregate("values", lit(0.0), lambda acc, x: acc + x).alias("sum")).show()
    +----+
    | sum|
    +----+
    |42.0|
    +----+
    >>> def merge(acc, x):
    ...     count = acc.count + 1
    ...     sum = acc.sum + x
    ...     return struct(count.alias("count"), sum.alias("sum"))
    >>> df.select(
    ...     aggregate(
    ...         "values",
    ...         struct(lit(0).alias("count"), lit(0.0).alias("sum")),
    ...         merge,
    ...         lambda acc: acc.sum / acc.count,
    ...     ).alias("mean")
    ... ).show()
    +----+
    |mean|
    +----+
    | 8.4|
    +----+
    """
    if finish is not None:
        return _invoke_higher_order_function(
            "ArrayAggregate", [col, zero], [merge, finish]
        )

    else:
        return _invoke_higher_order_function(
            "ArrayAggregate", [col, zero], [merge]
        )


# noinspection PyUnresolvedReferences
@since(3.1)
def zip_with(
    col1: Union[str, Column], col2: Union[str, Column], f: Any
) -> Column:
    """
    Merge two given arrays, element-wise, into a single array using a function.
    If one array is shorter, nulls are appended at the end to match the length of the longer
    array, before applying the function.
    :param col1: name of the first column or expression
    :param col2: name of the second column or expression
    :param f: a binary function ``(x1: Column, x2: Column) -> Column...``
        Can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([(1, [1, 3, 5, 8], [0, 2, 4, 6])], ("id", "xs", "ys"))
    >>> df.select(zip_with("xs", "ys", lambda x, y: x ** y).alias("powers")).show(truncate=False)
    +---------------------------+
    |powers                     |
    +---------------------------+
    |[1.0, 9.0, 625.0, 262144.0]|
    +---------------------------+
    >>> df = spark.createDataFrame([(1, ["foo", "bar"], [1, 2, 3])], ("id", "xs", "ys"))
    >>> df.select(zip_with("xs", "ys", lambda x, y: concat_ws("_", x, y)).alias("xs_ys")).show()
    +-----------------+
    |            xs_ys|
    +-----------------+
    |[foo_1, bar_2, 3]|
    +-----------------+
    """
    return _invoke_higher_order_function("ZipWith", [col1, col2], [f])


# noinspection PyUnresolvedReferences
@since(3.1)
def transform_keys(col: Union[str, Column], f: Any) -> Column:
    """
    Applies a function to every key-value pair in a map and returns
    a map with the results of those applications as the new keys for the pairs.
    :param col: name of column or expression
    :param f: a binary function ``(k: Column, v: Column) -> Column...``
        Can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([(1, {"foo": -2.0, "bar": 2.0})], ("id", "data"))
    >>> df.select(transform_keys(
    ...     "data", lambda k, _: upper(k)).alias("data_upper")
    ... ).show(truncate=False)
    +-------------------------+
    |data_upper               |
    +-------------------------+
    |[BAR -> 2.0, FOO -> -2.0]|
    +-------------------------+
    """
    return _invoke_higher_order_function("TransformKeys", [col], [f])


# noinspection PyUnresolvedReferences
@since(3.1)
def transform_values(col: Union[str, Column], f: Any) -> Column:
    """
    Applies a function to every key-value pair in a map and returns
    a map with the results of those applications as the new values for the pairs.
    :param col: name of column or expression
    :param f: a binary function ``(k: Column, v: Column) -> Column...``
        Can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([(1, {"IT": 10.0, "SALES": 2.0, "OPS": 24.0})], ("id", "data"))
    >>> df.select(transform_values(
    ...     "data", lambda k, v: when(k.isin("IT", "OPS"), v + 10.0).otherwise(v)
    ... ).alias("new_data")).show(truncate=False)
    +---------------------------------------+
    |new_data                               |
    +---------------------------------------+
    |[OPS -> 34.0, IT -> 20.0, SALES -> 2.0]|
    +---------------------------------------+
    """
    return _invoke_higher_order_function("TransformValues", [col], [f])


# noinspection PyUnresolvedReferences
@since(3.1)
def map_filter(col: Union[str, Column], f: Any) -> Column:
    """
    Returns a map whose key-value pairs satisfy a predicate.
    :param col: name of column or expression
    :param f: a binary function ``(k: Column, v: Column) -> Column...``
        Can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([(1, {"foo": 42.0, "bar": 1.0, "baz": 32.0})], ("id", "data"))
    >>> df.select(map_filter(
    ...     "data", lambda _, v: v > 30.0).alias("data_filtered")
    ... ).show(truncate=False)
    +--------------------------+
    |data_filtered             |
    +--------------------------+
    |[baz -> 32.0, foo -> 42.0]|
    +--------------------------+
    """
    return _invoke_higher_order_function("MapFilter", [col], [f])


# noinspection PyUnresolvedReferences
@since(3.1)
def map_zip_with(
    col1: Union[str, Column], col2: Union[str, Column], f: Any
) -> Column:
    """
    Merge two given maps, key-wise into a single map using a function.
    :param col1: name of the first column or expression
    :param col2: name of the second column or expression
    :param f: a ternary function ``(k: Column, v1: Column, v2: Column) -> Column...``
        Can use methods of :class:`pyspark.sql.Column`, functions defined in
        :py:mod:`pyspark.sql.functions` and Scala ``UserDefinedFunctions``.
        Python ``UserDefinedFunctions`` are not supported
        (`SPARK-27052 <https://issues.apache.org/jira/browse/SPARK-27052>`__).
    :return: a :class:`pyspark.sql.Column`
    >>> df = spark.createDataFrame([
    ...     (1, {"IT": 24.0, "SALES": 12.00}, {"IT": 2.0, "SALES": 1.4})],
    ...     ("id", "base", "ratio")
    ... )
    >>> df.select(map_zip_with(
    ...     "base", "ratio", lambda k, v1, v2: round(v1 * v2, 2)).alias("updated_data")
    ... ).show(truncate=False)
    +---------------------------+
    |updated_data               |
    +---------------------------+
    |[SALES -> 16.8, IT -> 48.0]|
    +---------------------------+
    """
    return _invoke_higher_order_function("MapZipWith", [col1, col2], [f])
