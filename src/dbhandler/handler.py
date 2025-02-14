import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from .exception import TableNotFoundError


class DBHandler:
    """
    SQLiteを操作するクラス
    """

    def __init__(self, DB_file_path: str | Path = Path(".db")) -> None:
        """
        コンストラクタ

        Parameters
        ----------
        DB_file_path : str | Path
            アクセスするDBファイル
        """
        self.__DB_file_path = Path(DB_file_path).resolve()
        self.__connector = sqlite3.connect(self.__DB_file_path)
        self.__cursor = self.__connector.cursor()

    def __del__(self) -> None:
        """
        デストラクタ
        接続しているファイルを切り離す
        """
        self.__cursor.close()
        self.__connector.close()

    def exec_query(self, query: str) -> list[Any]:
        """
        クエリを実行してリストで返す

        Parameters
        ----------
        query : str

        Returns
        -------
        list[Any]
        """
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def exec_query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        クエリを実行しpd.Dataframeで返す

        Parameters
        ----------
        query : str

        Returns
        -------
        pd.DataFrame
        """
        return pd.read_sql(query, self.__connector)

    def create_table(
        self, table_name: str, columns_dict: dict[str, type | None]
    ) -> None:
        """
        テーブルの作成（存在しない場合のみ）

        Parameters
        ----------
        table_name : str
            テーブル名
        columns_dict : dict[str, type | None]
            カラム名とそのデータ型を保持した辞書
            使用可能なデータ型は{int, float, str, None}
            例: {"id": int, "name": str}
        """
        type_dict = {int: "INTEGER", float: "REAL", str: "TEXT", None: "NULL"}
        columns = ", ".join(
            [f"{key} {type_dict[value]}" for key, value in columns_dict.items()]
        )
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.__cursor.execute(query)

        self.__commit()

    def create_table_from_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        """
        pd.DataFrameからテーブルを作成する
        既に存在する場合は上書きする

        Parameters
        ----------
        table_name : str
            テーブル名
        df : pd.DataFrame
            データ
        """
        df.to_sql(table_name, self.__connector, if_exists="replace", index=False)
        self.__commit()

    def rename_table(self, from_: str, to_: str) -> None:
        """
        テーブルの名前を変更する

        Parameters
        ----------
        from_ : str
            変更前のテーブル名
        to_ : str
            変更後のテーブル名

        Raises
        ------
        ValueError
            修正するテーブルが存在しなかった場合
        """
        if from_ not in self.tables:
            raise TableNotFoundError(from_)

        query = f"ALTER TABLE {from_} RENAME TO {to_}"
        self.__cursor.execute(query)

        self.__commit()

    def delete_duplicated(self, table_name: str) -> None:
        """
        テーブルの名前を変更する

        Parameters
        ----------
        table_name: str
            テーブル名
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)

        query = f"""
        SELECT DISTINCT
            *
        FROM
            {table_name}
        """
        df = self.exec_query_to_dataframe(query)
        self.create_table_from_dataframe(table_name, df)

        return None

    def delete_table(self, table_name: str) -> None:
        """
        テーブルを削除

        Parameters
        ----------
        table_name : str
            テーブル名

        Returns
        -------
        Self
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)

        query = f"DROP TABLE IF EXISTS {table_name}"
        self.__cursor.execute(query)

        self.__commit()

    def fetchall(self, table_name: str) -> list[Any]:
        """
        テーブル内のデータを全てリストで返す

        Parameters
        ----------
        table_name : str
            テーブル名

        Returns
        -------
        list[Any]
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)

        query = f"""
        SELECT
            *
        FROM
            {table_name}
        """
        return self.exec_query(query)

    def fetchall_to_dataframe(self, table_name: str) -> pd.DataFrame:
        """
        テーブル内のデータを全てpd.DataFrameで返す

        Parameters
        ----------
        table_name : str
            テーブル名

        Returns
        -------
        pd.DataFrame
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)

        query = f"""
        SELECT
            *
        FROM
            {table_name}
        """
        return self.exec_query_to_dataframe(query)

    def insert_records(self, table_name: str, values: list[list[Any]]) -> None:
        """
        テーブルにデータを格納する

        Parameters
        ----------
        table_name : str
            テーブル名
        values : list[list[Any]]
            保存するデータ

        Returns
        -------
        Self
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)
        if len(values) == 0:
            raise ValueError("add data is empty.")

        n_columns = len(values[0])
        query = f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * n_columns)})"
        self.__cursor.executemany(query, values)

        self.__commit()

        return None

    def insert_records_from_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        """
        テーブルにデータをpd.DataFrameから追加する

        Parameters
        ----------
        table_name : str
            テーブル名
        df : pd.DataFrame
            追加するデータ
        """
        if table_name not in self.tables:
            raise TableNotFoundError(table_name)
        if df.empty:
            raise ValueError("add data is empty.")


        df.to_sql(table_name, self.__connector, if_exists="append", index=False)
        self.__commit()

    def __commit(self) -> None:
        """
        DBにコミット
        """
        self.__connector.commit()

    @property
    def tables(self) -> list[str]:
        query = """
        SELECT
            name
        FROM
            sqlite_master
        WHERE
            type='table'
        """
        return self.exec_query_to_dataframe(query)["name"].to_list()

    @property
    def DB_file_path(self) -> Path:
        return self.__DB_file_path
