import re
from typing import List

from pyspark.sql import Column


class ColumnSpecWrapper:
    """
    This wrapper provides a nicely formatted string for column spec
    """

    def __init__(self, column_spec: Column) -> None:
        self.column_spec: Column = column_spec
        self.column_spec_formatted: str = self.to_debug_string()

    def __repr__(self) -> str:
        """
        Display for debugger

        :return: string representation for debugger
        """
        return self.to_debug_string()

    def to_debug_string(self) -> str:
        """
        Formats the column spec so () are on separate lines for easier reading
        :return: string
        """
        output: str = ""
        txt: str = str(self.column_spec)
        items: List[str] = re.split(r"([()])", txt)
        indent: int = 0
        item: str
        for item in items:
            if item != "":
                if item == ")":
                    indent -= 1
                output += "\n" + ("\t" * indent)
                output += f"{item}"
                if item == "(":
                    indent += 1
        return output
