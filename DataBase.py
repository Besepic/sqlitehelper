import sqlite3
import os

# Берём путь до файла с бдшкой
path = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/" )

class Data:

    """Инициализируем базу данных"""
    def __init__(self, dbname: str):

        # Подключаемся к БД
        self.db = sqlite3.connect(f'{path}/{dbname}.db')
        # Создаем курсор
        self.cur = self.db.cursor()
        print("БД работает")


    """ Создает таблицу с заданными столбцами """
    async def CreateTable(self, table_name:str, *args:str):

        #Пример:
        """
        'id', 'INT',
        'name', 'TEXT',
        'active', 'BOOL'
        """
        # выполняем SQL-запрос для создания таблицы
        columns = ", ".join([f"{arg} {args[i+1]}" for i, arg in enumerate(args) if i%2 == 0])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.db.execute(query)
        # сохраняем изменения и закрываем соединение
        self.db.commit()


    """ Заполняем таблицу """
    async def FillLine(self, table_name:str, *args):

        #вытаскиваем значение из бд
        self.cur.execute(f"SELECT * FROM {table_name}")
        first_column = self.cur.description[0][0]
        self.cur.execute(f"SELECT {first_column} FROM {table_name} WHERE {first_column} = ?", (tuple(args)[0],))        
        #если значения нет, то записываем
        if self.cur.fetchone() is None:
            quantity = (len(args)-1)*"?,"+"?"
            self.cur.execute(f"INSERT INTO {table_name} VALUES ({quantity})", (tuple(args)))
            #сохраняем
            self.db.commit()
            print("Данные записаны")


    """ Удаляем строку """
    async def DeleteLine(self, table_name:str, key:str, value:any, key_two:str = None, value_two:any = None):

        # если у нас 1 значение
        if key_two is None:
            # удаляем
            self.cur.execute(f"DELETE from {table_name} WHERE {key} = {value}")
        # если 2 значения
        else:
            # удаляем
            self.cur.execute(f"DELETE from {table_name} WHERE {key} = {value} AND {key_two} = {value_two}")
        # сохраняем
        self.db.commit()
        print("Данные удалены!")

    
    """ Возвращаем значение """
    async def ReturnValue(self, table_name:str, column:str , parameter:str = None):

        # вытаскиваем значение из таблицы
        self.cur.execute(f"SELECT {column} FROM {table_name} {parameter}")    
        result = list(self.cur.fetchall()[0])
        if len(result) == 1:
            result = result[0]
            if str(result).isdigit():
                result = int(result)
        #возвращаем значение
        return result
    

    """ Обновляем значение """
    async def UpdateValue(self, table_name:str, column:str, newvalue, parameter:str = None):
        
        # обновляем значение по столбцу
        self.cur.execute(f'UPDATE {table_name} SET {column} = "{newvalue}" {parameter}')      #parameter = f"WHERE id = {id}"
        self.db.commit()
        print("Данные обновлены")

    
    """ Возвращаем случайное значение """
    async def RandomValue(self, table_name:str, column:str, limit:int, parameter:str=None):
        
        result = list(self.cur.execute(f"SELECT {column} FROM {table_name} {parameter} ORDER BY RANDOM() LIMIT {limit}").fetchall())
        for i in range(len(result)):
            result[i] = list(result[i])[0]
        if len(result) == 1:
            result = result[0]
            if str(result).isdigit():
                result = int(result)
        #возвращаем значение
        return result