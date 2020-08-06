from typing import Any, Collection, Type, Optional

class ExType:
    def __init__(self,
        basetype: Type,
        *,
        val_range: Optional[Collection[int]] = None,
        len_range: Optional[Collection[int]] = None,
    ) -> None:
        self.basetype = basetype
        self.val_range = val_range
        self.len_range = len_range

    def is_valid(self, val:Any) -> bool:
        return (
            isinstance(val, self.basetype)
            and (self.val_range is None or val in self.val_range)
            and (self.len_range is None or len(val) in self.len_range)
        )

    def expect_valid(self, val:Any) -> None:
        if not self.is_valid(val):
            raise TypeError('The given value is not valid.')


class RangedType(ExType):
    def __init__(self, basetype:Type, val_range:Collection[int]):
        super().__init__(basetype, val_range=val_range)


class LenLimitedType(ExType):
    def __init__(self, basetype:Type, max_len:int):
        super().__init__(basetype, len_range=range(0, max_len + 1))
