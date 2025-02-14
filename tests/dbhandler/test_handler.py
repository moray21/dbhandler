import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest
from dbhandler.handler import DBHandler, TableNotFoundError


@pytest.fixture(scope="package")
def test_directory() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as td:
        yield Path(td)


@pytest.fixture(scope="package")
def testDB_path(test_directory: Path) -> Path:
    return test_directory / "test.db"


class TestDBHandler:
    @contextmanager
    def __add_table(self, tbl: DBHandler, table_name: str) -> Generator[None, None, None]:
        """
        テーブルを追加する

        Parameters
        ----------
        tbl : DBHandler
        table_name : str
            追加するテーブル名
        """
        try:
            df = pd.DataFrame({"id": [1, 2, 2], "name": ["te1", "te2", "te2"]})
            tbl.create_table_from_dataframe(table_name, df)
            yield
        finally:
            tbl.delete_table(table_name)

    @pytest.mark.parametrize(
        ["query", "expected_values"],
        [
            ("SELECT * FROM test", [(1, "te1"), (2, "te2"), (2, "te2")]),
            ("SELECT id FROM test", [(1,), (2,), (2,)]),
        ],
    )
    def test_exec_query(self, testDB_path: Path, query: str, expected_values: list) -> None:
        """queryに応じた結果を取得できること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            assert tbl.exec_query(query) == expected_values
        assert tbl.tables == []

    @pytest.mark.parametrize(
        ["query", "expected_df"],
        [
            (
                "SELECT * FROM test",
                pd.DataFrame({"id": [1, 2, 2], "name": ["te1", "te2", "te2"]}),
            ),
            ("SELECT id FROM test", pd.DataFrame({"id": [1, 2, 2]})),
        ],
    )
    def test_exec_query_to_dataframe(
        self, testDB_path: Path, query: str, expected_df: pd.DataFrame
    ) -> None:
        """queryに応じた結果をDataFrameで取得できること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            df = tbl.exec_query_to_dataframe(query)
            assert df.equals(expected_df)
        assert tbl.tables == []

    def test_create_and_delete_table(self, testDB_path: Path) -> None:
        """テーブルが正しく作れること"""
        tbl = DBHandler(testDB_path)

        # テーブルの作成
        table_name = "test"
        columns_dict = {"id": int, "name": str}
        tbl.create_table(table_name, columns_dict)
        assert tbl.tables == [table_name]

        # テーブルの削除
        tbl.delete_table(table_name)
        assert tbl.tables == []

    def test_create_table_from_dataframe(self, testDB_path: Path) -> None:
        """DataFrameからテーブルを作成する"""
        tbl = DBHandler(testDB_path)

        # テーブル作成
        table_name = "test"
        with self.__add_table(tbl, table_name):
            assert tbl.tables == [table_name]
        assert tbl.tables == []

    def test_delete_table_with_raises(self, testDB_path: Path) -> None:
        """削除するテーブルが存在しなかった場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(TableNotFoundError):
                tbl.delete_table("test-test")
        assert tbl.tables == []

    def test_rename_table(self, testDB_path: Path) -> None:
        """正しくリネームできること"""
        tbl = DBHandler(testDB_path)

        # テーブル作成
        before_table_name = "test"
        after_table_name = "test_test"
        with self.__add_table(tbl, before_table_name):
            tbl.rename_table(before_table_name, after_table_name)
            assert tbl.tables == [after_table_name]
            tbl.rename_table(after_table_name, before_table_name)
        assert tbl.tables == []

    def test_rename_table_with_raises(self, testDB_path: Path) -> None:
        """リネーム対象のテーブルがなかった場合に例外を発生させる"""
        tbl = DBHandler(testDB_path)
        with self.__add_table(tbl, "test123"):
            with pytest.raises(TableNotFoundError):
                tbl.rename_table("test", "test-test")
        assert tbl.tables == []

    def test_delete_duplicated(self, testDB_path: Path) -> None:
        """重複レコードを削除できること"""
        expected_df = pd.DataFrame({"id": [1, 2], "name": ["te1", "te2"]})

        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            tbl.delete_duplicated(table_name)
            df = tbl.fetchall_to_dataframe(table_name)
            assert df.equals(expected_df)
        assert tbl.tables == []

    def test_delete_duplicated_with_no_table(self, testDB_path: Path) -> None:
        """存在しないテーブルを指定した場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(TableNotFoundError):
                tbl.delete_duplicated("test-test")
        assert tbl.tables == []

    def test_fetchall(self, testDB_path: Path) -> None:
        """テーブル内の値を正しく取得できること"""
        expected_values = [(1, "te1"), (2, "te2"), (2, "te2")]

        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            assert tbl.fetchall(table_name) == expected_values
        assert tbl.tables == []

    def test_fetchall_with_raises(self, testDB_path: Path) -> None:
        """削除するテーブルが存在しなかった場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(TableNotFoundError):
                tbl.fetchall("test-test")
        assert tbl.tables == []

    def test_fetchall_to_dataframe(self, testDB_path: Path) -> None:
        """テーブル内の値をDataFrameで正しく取得できること"""
        expected_df = pd.DataFrame({"id": [1, 2, 2], "name": ["te1", "te2", "te2"]})

        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            df = tbl.fetchall_to_dataframe(table_name)
            assert df.equals(expected_df)
        assert tbl.tables == []

    def test_fetchall_to_dataframe_with_raises(self, testDB_path: Path) -> None:
        """削除するテーブルが存在しなかった場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(TableNotFoundError):
                tbl.fetchall_to_dataframe("test-test")
        assert tbl.tables == []

    def test_insert_records(self, testDB_path: Path) -> None:
        """正しくレコードを追加できること"""
        add_values = [(3, "te3"), (4, "te4")]
        expected_df = pd.DataFrame(
            {"id": [1, 2, 2, 3, 4], "name": ["te1", "te2", "te2", "te3", "te4"]}
        )

        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            tbl.insert_records(table_name, add_values)
            df = tbl.fetchall_to_dataframe(table_name)
            assert df.equals(expected_df)
        assert tbl.tables == []

    def test_insert_records_with_raises(self, testDB_path: Path) -> None:
        """削除するテーブルが存在しなかった場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(TableNotFoundError):
                tbl.insert_records("test-test", [(3, "te3"), (4, "te4")])
        assert tbl.tables == []

    def test_insert_records_with_no_data(self, testDB_path: Path) -> None:
        """追加データが空の場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(ValueError):
                tbl.insert_records("test", [])
        assert tbl.tables == []

    def test_insert_records_from_dataframe(self, testDB_path: Path) -> None:
        """正しくレコードをDataFrameから追加できること"""
        add_df = pd.DataFrame({"id": [3, 4], "name": ["te3", "te4"]})
        expected_df = pd.DataFrame(
            {"id": [1, 2, 2, 3, 4], "name": ["te1", "te2", "te2", "te3", "te4"]}
        )

        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            tbl.insert_records_from_dataframe(table_name, add_df)
            df = tbl.fetchall_to_dataframe(table_name)
            assert df.equals(expected_df)
        assert tbl.tables == []

    def test_insert_records_from_dataframe_with_raises(self, testDB_path: Path) -> None:
        """削除するテーブルが存在しなかった場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(TableNotFoundError):
                tbl.insert_records_from_dataframe("test-test", pd.DataFrame({"id": [3, 4], "name": ["te3", "te4"]}))
        assert tbl.tables == []

    def test_insert_records_from_dataframe_with_no_data(self, testDB_path: Path) -> None:
        """追加データが空の場合は例外を投げること"""
        tbl = DBHandler(testDB_path)
        table_name = "test"
        with self.__add_table(tbl, table_name):
            with pytest.raises(ValueError):
                tbl.insert_records_from_dataframe("test", pd.DataFrame())
        assert tbl.tables == []

    def test_tables_with_empty(self, testDB_path: Path) -> None:
        """テーブル名が正しく返ること"""
        tbl = DBHandler(testDB_path)
        assert tbl.tables == []

    def test_tables(self, testDB_path: Path) -> None:
        """テーブル名が正しく返ること"""
        tbl = DBHandler(testDB_path)
        assert tbl.tables == []

    def test_db_file_path(self, testDB_path: Path) -> None:
        """ファイルパスが正しく返ること"""
        tbl = DBHandler(testDB_path)
        assert tbl.DB_file_path == testDB_path
