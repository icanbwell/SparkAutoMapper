from typing import List


class FieldNode:
    def __init__(self, name: str) -> None:
        self.name: str = name
        self.children: List[FieldNode] = []
