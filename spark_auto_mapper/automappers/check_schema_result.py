from spark_data_frame_comparer.schema_comparer import SchemaComparerResult


class CheckSchemaResult:
    """
    Result of comparing schemas
    """

    def __init__(self, result: SchemaComparerResult) -> None:
        """


        :param result: comparison result
        """
        self.result: SchemaComparerResult = result

    def __str__(self) -> str:
        return str(self.result)
