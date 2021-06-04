from datetime import date, datetime
from typing import Union, List, Any, Dict

AutoMapperNativeSimpleType = Union[str, int, float, date, datetime, None]

AutoMapperNativeTextType = Union[str]

AutoMapperNativeNumberType = Union[str, int]

AutoMapperNativeBooleanType = Union[str, bool]

AutoMapperNativeDateType = Union[str, date, datetime]

AutoMapperNativeAmountType = Union[str, int, float]

AutoMapperNativeListType = Union[
    List[str], List[int], List[float], List[date], List[datetime], List[Dict[str, Any]]
]
