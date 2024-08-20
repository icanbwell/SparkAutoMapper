from pyspark.sql import SparkSession


class SparkHelpers:
    @staticmethod
    def spark_table_exists(
        spark_session: SparkSession, view: str, database: str = "default"
    ) -> bool:
        """
        :return:
        """
        return view in [
            row.tableName
            for row in spark_session.sql(f"SHOW TABLES IN {database}").collect()
        ]
