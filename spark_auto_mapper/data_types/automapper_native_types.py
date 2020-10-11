from datetime import date, datetime
from typing import Union, List, Any, Dict

AutoMapperSimpleType = Union[str, int, float, date, datetime]

AutoMapperNativeListType = Union[List[str], List[int], List[float], List[date], List[datetime], List[Dict[str, Any]]]
