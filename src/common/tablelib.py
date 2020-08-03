from typing import Dict, Iterable, Iterator, List

class Table:
    def __init__(self, column_names:List[str], rows:Iterable[List]) -> None:
        self.column_names = column_names
        self.column_name_to_index = {name: i for i, name in enumerate(self.column_names)}
        if len(self.column_names) != len(self.column_name_to_index):
            raise RuntimeError('There are columns of the same name.')
        self.rows = rows

    def __iter__(self) -> Iterator['Record']:
        return (Record(self.column_name_to_index, row) for row in self.rows)


class Record:
    def __init__(self, column_name_to_index:Dict[str, int], row:List) -> None:
        self.column_name_to_index = column_name_to_index
        self.row = row
    
    def __getitem__(self, column_name:str):
        return self.row[self.column_name_to_index[column_name]]

