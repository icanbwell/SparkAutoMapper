from typing import List, Dict, Optional

from pyspark.sql import DataFrame, Column

from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult


class AutoMapperBase:
    """
    Abstract Base class for AutoMappers
    """

    def __init__(self) -> None:
        pass

    def transform_with_data_frame(
        self, df: DataFrame, source_df: Optional[DataFrame], keys: List[str]
    ) -> DataFrame:
        """
        Internal function called by base class to transform the data frame


        :param df: destination data frame
        :param source_df: source data frame
        :param keys: key columns
        :return data frame after the transform
        """
        # implement in subclasses
        raise NotImplementedError

    def get_column_specs(self, source_df: Optional[DataFrame]) -> Dict[str, Column]:
        """
        Gets column specs (Spark expressions)


        :param source_df: source data frame

        :return: dictionary of column name, column expression
        """
        raise NotImplementedError

    def check_schema(
        self, parent_column: Optional[str], source_df: Optional[DataFrame]
    ) -> Optional[CheckSchemaResult]:
        """
        Checks the schema


        :param parent_column: parent column
        :param source_df: source data frame
        :return: result of checking schema
        """
        return None
