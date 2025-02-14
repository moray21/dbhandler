class TableNotFoundError(Exception):
    """
    テーブルが存在しない場合に投げるエラー
    """

    def __init__(self, table_name: str) -> None:
        self.__table_name = table_name

    def __str__(self) -> str:
        return f"table '{self.__table_name}' does not exists yet."
