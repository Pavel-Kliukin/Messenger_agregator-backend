import os
from pprint import pprint
import time
import json
import asyncio
import mimetypes
import subprocess
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, select, update, insert, delete, and_, or_


load_dotenv('user.env')  # load_dotenv загружает из файла user.env переменные среды

# Подключение к базе данных и настройка SQL Alchemy
# --------------------------------------------------
# logging('Подключение к БД')
host = os.environ.get('HOST')
# user = os.environ.get('USERNAME')
user = 'root'
password = os.environ.get('PASSWORD')
database = os.environ.get('DATABASE')
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}", isolation_level="AUTOCOMMIT")
connection = engine.connect()
metadata = MetaData()
metadata.reflect(bind=engine)  # Отображение ("подгрузка") всех уже существующих таблиц из БД
# --------------------------------------------------

while True:

    # Отслеживание команд в таблице commands из БД:
    commands_list = []
    commands = Table('commands', metadata)  # связываем переменную commands с таблицей 'commands' из БД
    accounts = Table('accounts', metadata)
    try:
        query = select(commands.c.id, commands.c.account_id).where(commands.c.status == 1)  # Выбираем из БД команды со status=0 (новые)
        commands_list = connection.execute(query).fetchall()  # Создаем список команд, скаченных из БД
    except Exception as e:
        pprint(f'Не удалось получить данные из таблицы commands базы данных. Попытка повторится в следующем цикле. \n{e}')
    if commands_list:  # Если список не пустой (в БД были команды), то отправляем команды на выполнение:
        for command in commands_list:
            command_id = command[0]
            account_id = command[1]
            slot = connection.execute(select(accounts.c.slot).where(accounts.c.id == account_id)).fetchone()[0]
            if slot == 1:  # Если аккаунт сейчас не занят выполнением другой команды
                # Переводим слот аккаунта в положение 3 (занят модулем start.py):
                connection.execute(update(accounts).where(accounts.c.id == account_id).values(slot=3))
                # Переводим команду в status=3 (в процессе выполнения):
                connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))
                # И запускаем выполнение модуля main.py, в который передаем id команды
                subprocess.Popen(['python', 'main.py', 'command', f'{command_id}'])

    # Отслеживание новых сообщений для активных аккаунтов (status = 1 в таблице accounts из БД
    accounts = Table('accounts', metadata)  # связываем переменную accounts с таблицей 'accounts' из БД
    # Создаем список активных и не занятых аккаунтов, скаченных из БД:
    query = select(accounts).where(and_(accounts.c.status == 1, accounts.c.slot == 1))
    active_accounts = connection.execute(query).fetchall()
    if active_accounts:  # Если есть активные аккаунты, то для каждого проверяем наличие новых сообщений в Telegram:
        for account in active_accounts:
            account_id = account[0]
            # Переводим слот аккаунта в положение 3 (занят модулем start.py):
            connection.execute(update(accounts).where(accounts.c.id == account_id).values(slot=3))
            # Запускаем выполнение модуля main.py, в который передаем id аккаунта
            subprocess.Popen(['python', 'main.py', 'account', f'{account_id}'])