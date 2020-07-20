from typing import Protocol
from typing import Any

class GType(Protocol):
    def __getitem__(self:'GType', *args, **kwargs): pass

class Hoge:
    pass


def main():
    print(
        GType,
        # GType[1],
        GType[int],
        GType[Hoge],
        GType[GType],
        GType[GType[Hoge]],
    )


if __name__ == "__main__":
    main()
