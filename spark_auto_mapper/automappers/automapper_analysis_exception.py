from typing import Optional, Any, List

from spark_auto_mapper.automappers.check_schema_result import CheckSchemaResult


class AutoMapperAnalysisException(Exception):
    """
    Exception thrown if there is an error in the AutoMapper
    """

    def __init__(
        self,
        msg: str,
        column_name: str,
        check_schema_result: Optional[CheckSchemaResult],
        column_values: Optional[List[Any]],
    ) -> None:
        """


        :param msg: message of error
        :param column_name: column that caused the error
        :param check_schema_result: result of checking the schemas
        :param column_values: value of columns
        """
        self.column_name: str = column_name
        self.check_schema_result: Optional[CheckSchemaResult] = check_schema_result
        self.column_values: Optional[List[Any]] = column_values
        super().__init__(msg)

    def __str__(self) -> str:
        """
        String representation
        """
        result: str = "AutoMapperAnalysisException: \n"
        if self.check_schema_result:
            result += str(self.check_schema_result)
        if self.column_values:
            result += "\n"
            result += "Sample source values: \n"
            result += "+------------------------+\n"
            result += f"   {self.column_name}  \n"
            result += "+------------------------+\n"
            for val in self.column_values:
                result += f"   {val}   \n"
            result += "+------------------------+\n"
        return result
