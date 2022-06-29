import argparse
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, select, update, insert, delete, and_


# Парсинг команд (аргументов), переданных из консоли
commands = ['get_avatars', 'get_all', 'get_contacts', 'get_dialogs', 'send_message', 'login', 'get_big_files']
parser = argparse.ArgumentParser(description='Command and arguments receiver')
parser.add_argument('command', type=str, help=f'Available commands: {", ".join(commands)}', choices=commands)
parser.add_argument('account_id', type=int, help='Account_id')
parser.add_argument('chat_id', type=str, nargs='?', default='', help='Chat_id')
parser.add_argument('-m', '--message', type=str, nargs='*', default='', help='Message text')
parser.add_argument('-f', '--files', type=str, nargs='*', default='', help='Paths to files to attach')
args = parser.parse_args()


# Подключение к базе данных и настройка SQL Alchemy
load_dotenv('user.env')  # load_dotenv загружает из файла user.env переменные среды
print('Подключение к БД')
host = os.environ.get('HOST')
user = 'root'
# user = os.environ.get('USERNAME')
password = os.environ.get('PASSWORD')
database = os.environ.get('DATABASE')
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}", isolation_level="AUTOCOMMIT")
connection = engine.connect()
metadata = MetaData()
metadata.reflect(bind=engine)  # Отображение всех таблиц из БД

# Отправка команды в БД в таблицу commands
commands = Table('commands', metadata)
arguments = None
if args.command == 'send_message':
    message_text = ' '.join(args.message)
    files = list(args.files)
    arguments = json.dumps({'to_channel': args.chat_id, 'message_text': message_text, 'files': files})

connection.execute(insert(commands).values(
    command=args.command,
    account_id=args.account_id,
    command_arguments=arguments,
    command_date=datetime.now()
))

print('Команда добавлена в базу данных.')

