from typing import Optional

from pyspark.sql import Column, DataFrame


class AutoMapperDataTypeBase:
    # noinspection PyMethodMayBeStatic
    def get_column_spec(
        self, source_df: DataFrame, current_column: Optional[Column]
    ) -> Column:
        """
        Gets the column spec for this automapper data type

        :param source_df: source data frame in case the automapper type needs that data to decide what to do
        :param current_column: (Optional) this is set when we are inside an array
        """
        raise NotImplementedError  # base classes should implement this

    # noinspection PyMethodMayBeStatic
    def get_value(
        self, value: 'AutoMapperDataTypeBase', source_df: DataFrame,
        current_column: Optional[Column]
    ) -> Column:
        """
        Gets the value for this automapper

        :param value: current value
        :param source_df: source data frame in case the automapper type needs that data to decide what to do
        :param current_column: (Optional) this is set when we are inside an array
        """
        assert isinstance(value, AutoMapperDataTypeBase)
        child: AutoMapperDataTypeBase = value
        return child.get_column_spec(
            source_df=source_df, current_column=current_column
        )

    def include_null_properties(self, include_null_properties: bool) -> None:
        pass  # sub-classes can implement if they support this
