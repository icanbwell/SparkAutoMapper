from typing import List, Optional
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import datediff, to_date, when

from spark_auto_mapper.data_types.data_type_base import AutoMapperDataTypeBase
from spark_auto_mapper.helpers.value_parser import AutoMapperValueParser
from spark_auto_mapper.type_definitions.defined_types import AutoMapperDateInputType
from spark_auto_mapper.data_types.column import AutoMapperDataTypeColumn


class AutoMapperPositiveDateDiffDataType(AutoMapperDataTypeBase):
    """
    Calculates the difference between two dates and returns the positive value if the difference is positive,
    otherwise returns None.

    This class ensures that the start_date and end_date are valid date or timestamp types.
    If the difference between the two dates is positive (end_date > start_date), the positive difference is returned.
    If the difference is zero or negative, the result is None.

    Example:
        If `start_date` is "2025-06-01" and `end_date` is "2025-06-10", the result will be `9`.
        If `start_date` is "2025-06-10" and `end_date` is "2025-06-01", the result will be `None`.
    """

    def __init__(
        self,
        end_date: AutoMapperDateInputType,
        start_date: AutoMapperDateInputType,
    ):
        super().__init__()
        # Parse the input values to ensure they are AutoMapperDataTypeBase
        self.start_date: AutoMapperDataTypeBase = (
            start_date
            if isinstance(start_date, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value=start_date)
        )
        self.end_date: AutoMapperDataTypeBase = (
            end_date
            if isinstance(end_date, AutoMapperDataTypeBase)
            else AutoMapperValueParser.parse_value(value=end_date)
        )

    def get_column_spec(
        self,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        start_date_spec = self._ensure_date(
            self.start_date, source_df, current_column, parent_columns
        )
        end_date_spec = self._ensure_date(
            self.end_date, source_df, current_column, parent_columns
        )
        date_diff_column = datediff(end_date_spec, start_date_spec)
        return when(date_diff_column > 0, date_diff_column).otherwise(None)

    def _ensure_date(
        self,
        value: AutoMapperDataTypeBase,
        source_df: Optional[DataFrame],
        current_column: Optional[Column],
        parent_columns: Optional[List[Column]],
    ) -> Column:
        """
        Ensures the given value is either a date . If not, converts it to a date.
        """
        column_spec = value.get_column_spec(
            source_df=source_df,
            current_column=current_column,
            parent_columns=parent_columns,
        )

        if (
            source_df is not None
            and isinstance(value, AutoMapperDataTypeColumn)
            and dict(source_df.dtypes).get(value.value) not in ["date", "timestamp"]
        ):
            # Convert to date instead of timestamp
            return to_date(column_spec)
        else:
            return column_spec

    @property
    def children(
        self,
    ) -> List[AutoMapperDataTypeBase]:
        return [self.end_date, self.start_date]
