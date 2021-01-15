from spark_data_frame_comparer.schema_comparer import SchemaComparerResult


class CheckSchemaResult:
    def __init__(self, result: SchemaComparerResult) -> None:
        self.result: SchemaComparerResult = result

    def __str__(self) -> str:
        return str(self.result)
