from pyspark.sql import Column


class AutoMapperDataTypeBase:
    # noinspection PyMethodMayBeStatic
    def get_column_spec(self) -> Column:
        raise NotImplementedError  # base classes should implement this

    # noinspection PyMethodMayBeStatic
    def get_value(self, value: 'AutoMapperDataTypeBase'):
        assert isinstance(value, AutoMapperDataTypeBase)
        child: AutoMapperDataTypeBase = value
        return child.get_column_spec()
