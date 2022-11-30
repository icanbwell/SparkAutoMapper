from typing import List

from pyspark.sql.types import StructType, StructField


class AnnotatedStructType:
    def __init__(self, fields: List[StructField]) -> None:
        pass

    @staticmethod
    def parse(struct_type: StructType) -> "AnnotatedStructType":
        pass
