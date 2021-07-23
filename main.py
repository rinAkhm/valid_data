import json
import jsonschema
from typing import Any
import sqlite3
import os


def read_json(file: str) -> Any:
    """Функция для чтения json файлов."""
    try:
        with open(f"{file}", "r", encoding="utf-8") as js:
            text = json.load(js)
            return text
    except Exception as e:
        print(f' " Ошибка при чтении файле: {e} " ')
        return False


def check_json(param1: str, param2: str) -> Any:
    """Проверяет json на валидность при помощи jsonshema."""
    goods_json = read_json(param1)
    goods_schema = read_json(param2)
    try:
        jsonschema.validate(goods_json, goods_schema)
        return goods_json
    except Exception as e:
        print(f"Ошибка при проверке json: \n{e}")
        return False


def create_tables(con: Any, cursor: Any) -> Any:
    """Создаются таблицы goods и shops_goods."""
    try:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS goods(
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    name TEXT NOT NULL,
                                    package_height REAL NOT NULL,
                                    package_width REAL NOT NULL);"""
        )

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS shops_goods(
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    id_good INTEGER NOT NULL,
                                    location TEXT NOT NULL,
                                    amount INTEGER NOT NULL,
                                    FOREIGN KEY (id_good) REFERENCES Goods(id));"""
        )

    except Exception as e:
        print(f"Invalid table: {e}")
        return False

    con.commit()
    con.close()
    return True


def connect_db(database: str) -> Any:
    """Соединение с database Goods."""
    con = sqlite3.connect(f"{database}")
    cursor = con.cursor()
    return con, cursor


def recording_data(con: Any, cursor: Any, data: dict) -> Any:
    """Записывает или обновляет данные в таблицу."""
    goods = []
    shop_goods = []
    for key, value in data.items():
        if key == "name":
            goods.append(value)
        elif key == "id":
            id_product = value
            goods.append(value)
        elif isinstance(value, dict):
            goods.append(value["width"])
            goods.append(value["height"])
        elif isinstance(value, list):
            for index in range(len(value)):
                temp = []
                temp.append(id_product)
                temp.append((value[index]["location"]))
                temp.append((value[index]["amount"]))
                shop_goods.append(tuple(temp))
    del data
    del temp
    try:
        cursor.execute(f"SELECT * FROM goods WHERE ID={id_product}")
        records = cursor.fetchall()
        if records:
            cursor.execute(
                "UPDATE goods SET id = ?,"
                " name = ?,"
                " package_height =?,"
                " package_width = ?"
                f" WHERE id = {id_product}",
                goods,
            )
        else:
            cursor.execute(
                "INSERT INTO goods (id, name, package_width, package_height) VALUES (?,?,?,?);",
                goods,
            )

        cursor.execute(f"SELECT * FROM shops_goods WHERE id_good={id_product}")
        records = cursor.fetchall()
        if records:
            for i in range(len(shop_goods)):
                location = shop_goods[i][1]
                cursor.execute(
                    "UPDATE shops_goods SET id_good = ?, "
                    "location = ?,"
                    "amount = ?"
                    f"WHERE id_good = {id_product} and location = '{location}';",
                    shop_goods[i],
                )
        else:
            cursor.executemany(
                "INSERT INTO shops_goods (id_good, location, amount) VALUES (?,?,?);",
                shop_goods,
            )

    except Exception as e:
        print(e)
        return False
    con.commit()
    con.close()
    return True


def main() -> str:
    """Управляет процессом записи."""
    directory = os.listdir(f"{os.path.dirname(__file__)}")
    if "Goods.db" not in directory:
        con, cursor = connect_db(database="Goods.db")
        create_tables(con, cursor)
        print("Таблицы были созданы успешно")
    file1 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "goods_file.json")
    file2 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "goods.schema.json"
    )
    valid_json = check_json(file1, file2)
    if valid_json:
        con, cursor = connect_db(database="Goods.db")
        if recording_data(con=con, cursor=cursor, data=valid_json) is True:
            return "Запись успешно добавлена"
        else:
            return "Запись не была добавлена"
    else:
        return "Json не валидный"


if __name__ == "__main__":
    main()
