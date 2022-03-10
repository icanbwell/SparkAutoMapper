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

    def to_string(self, include_info: bool = False) -> str:
        return self.result.to_string(include_info=include_info)
