from infokind import InfoKind
from typing import List, Dict, Tuple, Union
import pprint

class Src2Dest(object):
    def __init__(self, kind: InfoKind, key: List[str], source_table: str, source_column: str, dest_table: Union[str, None], dest_column: Union[str, None]):
        self.kind = kind
        self.key = key
        self.source_table = source_table
        self.source_column = source_column
        self.dest_table = dest_table
        self.dest_column = dest_column
    def __str__(self):
        return 'Src2Dest(\n  ' + pprint.pformat(self.__dict__) +')'