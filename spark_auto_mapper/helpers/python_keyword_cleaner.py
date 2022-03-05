class PythonKeywordCleaner:
    """
    This class fixes python keywords by appending a _ and can recover the original by removing the _
    This is needed so Python does not complain that we are using keywords such as "id"
    """

    python_keywords = [
        "False",
        "None",
        "True",
        "and",
        "as",
        "assert",
        "async",
        "await",
        "break",
        "class",
        "continue",
        "def",
        "del",
        "elif",
        "else",
        "except",
        "finally",
        "for",
        "from",
        "global",
        "if",
        "import",
        "in",
        "is",
        "lambda",
        "nonlocal",
        "not",
        "or",
        "pass",
        "raise",
        "return",
        "try",
        "while",
        "with",
        "yield",
        "id",
        "type",
        "List",
    ]

    @staticmethod
    def to_python_safe(name: str) -> str:
        """
        Checks if name is a Python keyword>  If yes, it appends a _


        :param name: name to clean
        :return cleaned name
        """
        result: str = (
            f"{name}_" if name in PythonKeywordCleaner.python_keywords else name
        )
        if result and result[0].isdigit():
            result = "_" + result
        return result

    @staticmethod
    def from_python_safe(name: str) -> str:
        """
        Checks if name ends with _ and if the name minus the last character is a python keyword
        If so it returns the name minus the last character


        :param name: name to check
        :return name without the _ if name is a python keyword
        """
        if not name or len(name) <= 1:
            return name
        result: str = (
            name[:-1]
            if name[-1] == "_" and name[:-1] in PythonKeywordCleaner.python_keywords
            else name
        )
        if result and result[1] == "_":
            result = result[1:]

        return result
