from pyspark.sql import Column, DataFrame


class AutoMapperDataTypeBase:
    # noinspection PyMethodMayBeStatic
    def get_column_spec(self, source_df: DataFrame) -> Column:
        raise NotImplementedError  # base classes should implement this

    # noinspection PyMethodMayBeStatic
    def get_value(
        self, value: 'AutoMapperDataTypeBase', source_df: DataFrame
    ) -> Column:
        assert isinstance(value, AutoMapperDataTypeBase)
        child: AutoMapperDataTypeBase = value
        return child.get_column_spec(source_df=source_df)
