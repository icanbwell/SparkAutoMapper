from pyspark.sql.context import SQLContext


class SparkHelpers:
    @staticmethod
    def spark_table_exists(sql_ctx: SQLContext, view: str) -> bool:
        """
        :return:
        """
        # noinspection PyBroadException
        return view in sql_ctx.tableNames()
