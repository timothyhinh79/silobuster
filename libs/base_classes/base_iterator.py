class BaseIterator:

    def __init__(self, group_list=None, attr_key='id'):
        self._index = 0
        try:
            if group_list is None:
                self.__group_list = []
            else:
                self.__group_list = group_list

        except:
            self.__group_list = []

        self.__attr_key = attr_key

    def __next__(self) -> object:
        if self._index < len(self.__group_list):
            result = self.__group_list[self._index]
            self._index += 1
            return result

        raise StopIteration


    def __iter__(self) -> object:
        return self


    def __len__(self) -> object:
        return len(self.__group_list)

    def __getitem__(self, key) -> object:
        try:
            for group in self.__group_list:
                if getattr(group, self.__attr_key) == key: return group

        except:
            return None

    def reset_iterator(self):
        self._index = 0


    def append(self, group) -> None:
        self.__group_list.append(group)


    def exists(self, key) -> bool:
        if self[key] is None: return False
        return True

    @property
    def objList(self) -> list:
        return self.__group_list


