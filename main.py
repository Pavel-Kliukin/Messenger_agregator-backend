import os
import time
import json
import asyncio
import mimetypes
from dotenv import load_dotenv
from datetime import datetime, timezone
from sqlalchemy import create_engine, MetaData, Table, select, update, insert, delete, and_
from telethon import TelegramClient
from telethon.tl.types import User, Dialog, Channel, Chat, UserStatusOffline, UserStatusRecently, \
    UserStatusLastMonth, UserStatusLastWeek, PeerChannel, PeerChat, PeerUser, Message, MessageMediaUnsupported, \
    MessageMediaWebPage, MessageMediaPhoto, MessageMediaDocument, MessageMediaPoll, DocumentAttributeFilename
from telethon.utils import get_display_name
from telethon.errors import SessionPasswordNeededError, MediaInvalidError


# Авторизация пользователя в Телеграм
async def login(account_id, connection, metadata):
    accounts = Table('accounts', metadata)
    connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=3))  # перевод аккаунта в status=3 (идет процесс логина)
    client = await connect_to_telegram(account_id)
    await client.connect()
    # Если не авторизованы в Telegram, то авторизируемся:
    if not await client.is_user_authorized():
        phone = '+' + connection.execute(select([accounts.c.phone]).where(accounts.c.id == account_id)).fetchone()[0]
        await client.send_code_request(phone)
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=4))
        try:
            print('Ожидание кода из базы данных в течение 60 секунд...')
            for i in range(10):  # Ждём 10 раз по 6 сек, чтобы получить код из БД
                time.sleep(6)
                code = connection.execute(select([accounts.c.code]).where(accounts.c.id == account_id)).fetchone()[0]
                if code:
                    await client.sign_in(phone, code)  # Отправляем Телеграму код
                    # и удаляем этот код в таблице accounts:
                    connection.execute(update(accounts).where(accounts.c.id == account_id).values(code=None))
                    break
                if i == 9:
                    print('Ожидание кода завершено безрезультатно. Нужно запустить авторизацию повторно.')
                    connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=3))  # перевод аккаунта в status=3 (идет процесс логина)
        except SessionPasswordNeededError:  # Если стоит двухфакторная верификация
            await client.sign_in(password=input('У вас двухфакторная верификация. Введите свой пароль:'))
    if await (client.is_user_authorized()):
        print('Login to Telegram completed successfully')
        print('У вас есть 60 секунд, если хотите запустить скачивание всех каналов ()')
        time.sleep(60)  # Задержка времени, чтобы успеть запустить скачивание всех диалогов
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=1))
        await client.disconnect()
    else:
        print('Login to Telegram failed')
        await client.disconnect()


# Устанавление соединения с Telegram
async def connect_to_telegram(account_id):
    api_id = int(os.environ.get('API_ID'))
    api_hash = os.environ.get('API_HASH')
    client = TelegramClient(str(account_id), api_id, api_hash)
    return client


# Добавление ОДНОГО диалога (с человеком или ботом), чата или канала в таблицу channels базы данных
async def add_to_channels(entity, account_id, connection, metadata):
    channels = Table('channels', metadata)

    # Проверка, есть ли уже такая запись в таблице channels:
    query = select(channels).where(
        and_(
            channels.c.channel == entity.id,
            channels.c.account_id == account_id)
    )
    record_exists = connection.execute(query).fetchone()
    if not record_exists:  # Если записи не существует, то добавляем её

        # Определение значения колонки type_channel для таблицы channels:
        entity_type = entity.to_dict()['_']  # может быть User, Chat (ChatForbidden) или Channel (ChannelForbidden)
        if entity_type is 'User' and entity.bot is False:
            channel_type = 0
        elif entity_type is 'User' and entity.bot is True:
            channel_type = 4
        elif entity_type is 'Channel':
            channel_type = 3
        elif entity_type is 'Channel' and (entity.megagroup or entity.gigagroup):
            channel_type = 2
        else:
            channel_type = 1

        # Создание комманды в БД на добавление записи в таблицу channels:
        query = insert(channels).values(
            account_id=account_id,
            channel=entity.id,
            name=get_display_name(entity) if entity_type is 'User' else entity.title,
            username=entity.username if entity_type not in ('Chat', 'ChatForbidden', 'ChatEmpty') else None,
            phone=entity.phone if entity_type is 'User' else None,
            type_channel=channel_type,
            can_view_participants=entity.participants_count if entity_type in ('Chat', 'Channel') else None,
            created_at=entity.date if entity_type in ('Chat', 'Channel') else None
        )
        connection.execute(query)  # Отправление команды в БД


# Скачивание файла из сообщения и занесениие информации в БД (большие файлы НЕ скачиваются, о них только заносится инф.)
async def file_download(message, dialog, client, account_id, connection, metadata):
    table_messages = Table('messages', metadata)
    message_files = Table('message_files', metadata)

    if isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):

        # Скаивание файла на сервер, если он не превышает определенный размер
        file_path = None
        downloaded_at = None
        is_downloaded = 0
        if isinstance(message.media, MessageMediaPhoto) or (
                isinstance(message.media, MessageMediaDocument) and message.media.document.size < 5000000):
            path = f'storage/{dialog.entity.id}/{message.id}'
            file_path = await client.download_media(message.media, path)
            file_path = file_path.replace('\\', '/')
            downloaded_at = datetime.now()
            is_downloaded = 1

        # Занесение информации о файле в таблицу message_files (независимо от того, был он скачен на сервер или нет)
        file_name = None
        if isinstance(message.media, MessageMediaDocument):
            for attribute in message.media.document.attributes:
                if isinstance(attribute, DocumentAttributeFilename):
                    file_name = attribute.file_name

        file_extension = None
        if file_path:
            file_extension = os.path.splitext(file_path)[1].replace('.', '')
        if file_name and not file_extension:
            file_extension = os.path.splitext(file_name)[1].replace('.', '').lower()

        query = insert(message_files).values(
            message_id=message.id,
            account_id=account_id,
            channel_id=dialog.entity.id,
            channel_name=get_display_name(dialog.entity) if isinstance(dialog.entity, User) else dialog.entity.title,
            file_tg_id=message.media.document.id if isinstance(message.media, MessageMediaDocument) else message.media.photo.id,
            file_name=file_name,
            fn_our=file_path,
            is_downloaded=is_downloaded,
            src=message.media.to_json(),
            fn_ext=file_extension,
            mime=mimetypes.guess_type(file_path)[0] if file_path else message.media.document.mime_type,
            from_id=message.from_id.user_id if message.from_id else dialog.entity.id,
            from_name=get_display_name(await client.get_entity(message.from_id.user_id if message.from_id else dialog.entity.id)),
            created_at=downloaded_at if downloaded_at else datetime.now(),
            downloaded_at=downloaded_at if downloaded_at else None
        )
        connection.execute(query)  # Отправление команды в БД

        # Занесение информации о файле в таблицу messages
        if file_path:
            size = os.path.getsize(file_path)
            files = {'filename': file_path, "size": size, "type": file_extension}
        else:
            size = message.media.document.size if isinstance(message.media, MessageMediaDocument) else 'Unknown'
            files = {"size": size, "type": file_extension}
        files = json.dumps(files)

        connection.execute(update(table_messages).where(and_(
            table_messages.c.message_id == message.id,
            table_messages.c.channel_id == dialog.entity.id)
        ).values(files=files))
        print("Информация о файле занесена")


# Скачивание ОДНОЙ аватарки (с удалением старой, если она существовала)
async def avatar_download(entity, client, connection, metadata):
    channels = Table('channels', metadata)
    messenger_users = Table('messenger_users', metadata)
    try:
        if entity.photo:
            avatar_path = f'avatars/{entity.id}.jpg'
            try:
                if os.path.isfile(avatar_path):  # Если аватарка уже была скачана ранее, то удаляем её
                    os.remove(avatar_path)
                    print('Старая аватарка удалена')
                await client.download_profile_photo(entity, f'avatars/{entity.id}')
            except PermissionError:
                print('Не удалось удалить старую аватарку. Отказано в доступе.')

            # Внесение адреса аватарки в БД в таблицы channels и messenger_users
            # Если объект с таким аватаром есть в базе channels, то заносим в таблицу адрес аватарки
            if connection.execute(select(channels).where(channels.c.channel == entity.id)).fetchone():
                connection.execute(update(channels).where(channels.c.channel == entity.id).values(fn_avatar=f'avatars/{entity.id}.jpg'))
            # Если объект с таким аватаром есть в базе messenger_users, то заносим в таблицу адрес аватарки
            if connection.execute(select(messenger_users).where(messenger_users.c.peer_id == entity.id)).fetchone():
                connection.execute(update(messenger_users).where(messenger_users.c.peer_id == entity.id).values(fn_avatar=f'avatars/{entity.id}.jpg'))

            print('Аватарка скачена')
    except AttributeError:
        pass


# Скачивание всех аватарок на сервер
async def get_avatars(account_id, connection, metadata):
    client = await connect_to_telegram(account_id)
    await client.connect()
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        await avatar_download(entity, client, connection, metadata)
    await client.disconnect()


# Добавление ВСЕХ диалогов (с человеком или ботом), чатов и каналов пользователя в таблицу channels базы данных
async def get_all(account_id, connection, metadata):
    client = await connect_to_telegram(account_id)
    await client.connect()
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        await add_to_channels(entity, account_id, connection, metadata)
        await avatar_download(entity, client, connection, metadata)  # Скачивание (обновление) аватара:
    await client.disconnect()


# Добавление только контактов пользователя в БД в таблицу messenger_users
async def get_contacts(account_id, connection, metadata):
    client = await connect_to_telegram(account_id)
    await client.connect()
    dialogs = await client.get_dialogs()

    #  Создание списка диалогов, состоящих только из контактов пользователя:
    dialogs_of_contacts = list(filter(lambda x: isinstance(x.entity, User) and x.entity.contact, dialogs))

    for dialog in dialogs_of_contacts:
        contact = dialog.entity

        # Определение значения колонки status для таблицы messenger_users:
        status = 0  # 0 - Online, 1 - Offline, 2 - Recently, 3 - Last week, 4 - Last Month
        if isinstance(contact.status, UserStatusOffline):
            status = 1
        elif isinstance(contact.status, UserStatusRecently):
            status = 2
        elif isinstance(contact.status, UserStatusLastWeek):
            status = 3
        elif isinstance(contact.status, UserStatusLastMonth):
            status = 4

        # Создание комманды в БД на добавление записи в таблицу messenger_users:
        messenger_users = Table('messenger_users', metadata)
        query = insert(messenger_users).values(
            peer_id = contact.id,
            first_name=contact.first_name,
            last_name=contact.last_name,
            photo_id=contact.photo.photo_id if contact.photo else None,
            username=contact.username,
            status=status,
            created_at=datetime.now()
        )
        connection.execute(query)  # Отправление команды в БД

        await avatar_download(contact, client, connection, metadata)  # Скачивание (обновление) аватара:

    await client.disconnect()


# Скачивание сообщений из всех диалогов в БД в таблицу messages
async def get_dialogs(account_id, connection, metadata):
    client = await connect_to_telegram(account_id)
    await client.connect()
    accounts = Table('accounts', metadata)
    table_messages = Table('messages', metadata)
    channels = Table('channels', metadata)
    start_time = datetime.now()

    async for dialog in client.iter_dialogs():

        # Провекрка занесен ли канал, из которого мы скачиваем сообщения, в БД в таблицу channels:
        channel_id = connection.execute(select([channels.c.id]).where(channels.c.channel == dialog.entity.id)).fetchone()
        if not channel_id:  # Если канала в БД нет, то добавляем его в таблицу channels
            print(f'Канала с id={dialog.entity.id} нет в таблице channels. Добавляем его в таблицу.')
            await add_to_channels(dialog.entity, account_id, connection, metadata)
            await avatar_download(dialog.entity, client, connection, metadata)  # скачивание и внесение в БД аватара

        # last_check_date - время последнего обновления сообщений диалога (берётся из канала channels):
        query = select([channels.c.lst_msgs_upd]).where(and_(
            channels.c.channel == dialog.entity.id,
            channels.c.account_id == account_id)
        )
        answer_from_db = connection.execute(query).fetchone()[0]
        last_check_date = answer_from_db if answer_from_db else datetime(1982, 11, 5)
        print(dialog.name)
        # Заносим новое время обновления сообщений диалога в таблицу channels
        connection.execute(update(channels).where(and_(
            channels.c.channel == dialog.entity.id,
            channels.c.account_id == account_id)
        ).values(lst_msgs_upd=datetime.now()))
        async for message in client.iter_messages(dialog):
            msg_date = message.date.replace(tzinfo=timezone.utc).astimezone(tz=None)
            if msg_date.timestamp() > last_check_date.timestamp():  # Если дата сообщения > даты последней проверки сообщений, то скачиваем его
                # Создание комманды в БД на добавление сообщения в таблицу messages:
                try:
                    from_id = message.from_id.user_id if message.from_id else dialog.entity.id
                    query = insert(table_messages).values(
                        bot_id=dialog.entity.id if isinstance(dialog.entity, User) and dialog.entity.bot else None,
                        account_id=account_id,
                        message_id=str(message.id),
                        channel_id_our=connection.execute(select([channels]).where(channels.c.channel == dialog.entity.id)).fetchone()[0],
                        channel_id=dialog.entity.id,
                        channel_name=connection.execute(select([channels]).where(channels.c.channel == dialog.entity.id)).fetchone()[4],
                        msg_date=msg_date,
                        napr=1 if message.out else 2,
                        from_id=from_id,
                        from_name=get_display_name(await client.get_entity(message.from_id.user_id if message.from_id else dialog.entity.id)),
                        message=message.message,
                        src=json.dumps(message.to_dict(), default=str, ensure_ascii=False),
                        created_at=datetime.now()
                    )
                    connection.execute(query)  # Отправление команды в БД
                except AttributeError:
                    print(AttributeError)
                    print('Чтото не так с from_id из сообщения:')
                    print(message)
                # Если в сообщении есть файл, то скачативаем его
                if message.media:
                    await file_download(message, dialog, client, account_id, connection, metadata)

                entity = await client.get_entity(from_id)
                await avatar_download(entity, client, connection, metadata)  # скачивание и внесение в БД аватара
            else:
                break
    end_time = datetime.now()
    print(f'Время проверки и скачивания сообщений для пользователя с id {account_id} составило {end_time - start_time}')
    # Добавление в таблицу accounts даты последней проверки новых сообщений:
    connection.execute(update(accounts).where(accounts.c.id == account_id).values(new_messages_last_check=end_time))
    await client.disconnect()


# Скачивание БОЛЬШИХ файлов
async def get_big_files(account_id, connection, metadata):
    client = await connect_to_telegram(account_id)
    await client.connect()
    table_messages = Table('messages', metadata)
    message_files = Table('message_files', metadata)

    # Выбираем из БД список сообщений у которых есть не скаченный файл:
    not_downloaded_files = connection.execute(select(
        message_files.c.channel_id,
        message_files.c.message_id,
        message_files.c.file_name
    ).where(and_(
        message_files.c.is_downloaded == 0,
        message_files.c.account_id == account_id))).fetchall()
    print(not_downloaded_files)

    for entry in not_downloaded_files:

        # Скаивание файла на сервер
        dialog_id, message_id, file_name = entry[0], entry[1], entry[2]
        message = await client.get_messages(dialog_id, ids=message_id)
        path = f'storage/{dialog_id}/{message_id}'
        print(f'Началась загрузка файла {file_name}')
        file_path = await client.download_media(message.media, path)
        file_path = file_path.replace('\\', '/')
        downloaded_at = datetime.now()

        # Изменение информации о файле в таблице message_files
        file_extension = None
        if file_path:
            file_extension = os.path.splitext(file_path)[1].replace('.', '')
        if file_name and not file_extension:
            file_extension = os.path.splitext(file_name)[1].replace('.', '').lower()

        connection.execute(update(message_files).where(and_(
            message_files.c.message_id == message_id,
            message_files.c.channel_id == dialog_id,
            message_files.c.account_id == account_id)
        ).values(
            fn_our=file_path,
            is_downloaded=1,
            fn_ext=file_extension,
            downloaded_at=downloaded_at))

        # Изменение информации о файле в таблице messages
        size = os.path.getsize(file_path)
        files = {'filename': file_path, "size": size, "type": file_extension}
        files = json.dumps(files)

        connection.execute(update(table_messages).where(and_(
            table_messages.c.message_id == message_id,
            table_messages.c.channel_id == dialog_id,
            table_messages.c.account_id == account_id)
        ).values(
            files=files))

        print("Файл скачен. Информация занесена в базу данных.")
    await client.disconnect()


# Подготовка сообщения к отправке (добавление его в таблицу messages_send)
def send_message(account_id, arguments, connection, metadata):
    commands = Table('commands', metadata)
    messages_send = Table('messages_send', metadata)

    arguments = json.loads(arguments)
    print(arguments)
    query = insert(messages_send).values(
        channel_id=str(arguments['to_channel']),
        date_msg=datetime.now(),
        from_account_id=account_id,
        message=arguments['message_text'],
        files=json.dumps(arguments['files']) if arguments['files'] else None
    )
    connection.execute(query)


# Отправка сообщений из таблицы messages_send)
async def finish_send(account_id, connection, metadata):
    client = await connect_to_telegram(account_id)
    await client.connect()
    messages_send = Table('messages_send', metadata)

    message_arguments= connection.execute(select(
        messages_send.c.id,
        messages_send.c.channel_id,
        messages_send.c.message,
        messages_send.c.files
    ).where(messages_send.c.from_account_id == account_id)).fetchall()

    for ma in message_arguments:
        message_id = ma[0]
        to_channel = ma[1]
        text = ma[2]
        files = json.loads(ma[3]) if ma[3] else None

        if files:
            media, documents = [], []
            # Разбиваем файлы на 2 типа (медиа и остальные):
            # Будем пробовать отправить одним пакетом сообщение + медиафайлы и вторым пакетом остальные файлы
            for file in files:
                file_extension = os.path.splitext(file)[1].replace('.', '').lower()
                if file_extension in ('jpg', 'jpeg', 'png', 'mp4', 'mov'):
                    media.append(file)
                else:
                    documents.append(file)
            if media:
                try:
                    await client.send_message(to_channel, text, file=media)
                except MediaInvalidError:  # Когда файлы не удается отправить одним пакетом, отправляем по одному
                    await client.send_message(to_channel, text)
                    for file in media:
                        await client.send_file(to_channel, file)
                if documents:
                    try:
                        await client.send_file(to_channel, file=documents)
                    except MediaInvalidError:  # Когда файлы не удается отправить одним пакетом, отправляем по одному
                        for file in documents:
                            await client.send_file(to_channel, file)
            else:
                try:
                    await client.send_message(to_channel, text, file=documents)
                except MediaInvalidError:  # Когда файлы не удается отправить одним пакетом, отправляем по одному
                    await client.send_message(to_channel, text)
                    for file in documents:
                        await client.send_file(to_channel, file)
        else:
            await client.send_message(to_channel, text)
        # Удаление отправленного сообщения из таблицы messages_send:
        connection.execute(delete(messages_send).where(messages_send.c.id == message_id))
        print(f'Сообщение из таблицы messages_send с id={message_id} отправлено')
    await client.disconnect()


async def main():
    load_dotenv('user.env')  # load_dotenv загружает из файла user.env переменные среды

    # Подключение к базе данных и настройка SQL Alchemy
    # --------------------------------------------------
    print('Подключение к БД')
    host = os.environ.get('HOST')
    # user = 'root'
    user = os.environ.get('USERNAME')
    password = os.environ.get('PASSWORD')
    database = os.environ.get('DATABASE')
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}", isolation_level="AUTOCOMMIT")
    connection = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)  # Отображение ("подгрузка") всех уже существующих таблиц из БД
    # --------------------------------------------------

    while True:

        # Отслеживание команд в таблице commands из БД:
        # --------------------------------------------------
        commands_list = []
        commands = Table('commands', metadata)  # связываем переменную commands с таблицей 'commands' из БД
        try:
            query = commands.select().where(commands.c.status == 0)  # Выбираем из БД команды со status=0 (новые)
            commands_list = connection.execute(query)  # Создаем список команд, скаченных из БД
        except:
            print('Не удалось получить данные из таблицы commands базы данных. Попытка повторится в следующем цикле.')
        if commands_list:  # Если список не пустой (в БД были команды), то отправляем команды на выполнение:
            for command in commands_list:
                command_id = command[0]
                command_name = command[1]
                account_id = command[2]
                command_args = command[3]
                # Перевод команды в status=3 (в процессе выполнения):
                connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))
                # -------------
                if command_name == 'login':
                    try:
                        await login(account_id, connection, metadata)
                        # Перевод команды в status=1 (выполнена):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                    except:
                        print(f'При выполнении команды {command_name} для аккаунта с id={account_id} возникли проблемы')
                        # Перевод команды в status=2 (возникла проблема):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
                elif command_name == 'get_avatars':
                    try:
                        await get_avatars(account_id, connection, metadata)
                        # Перевод команды в status=1 (выполнена):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                    except:
                        print(f'При выполнении команды {command_name} для аккаунта с id={account_id} возникли проблемы')
                        # Перевод команды в status=2 (возникла проблема):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
                elif command_name == 'get_all':
                    try:
                        await get_all(account_id, connection, metadata)
                        # Перевод команды в status=1 (выполнена):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                    except:
                        print(f'При выполнении команды {command_name} для аккаунта с id={account_id} возникли проблемы')
                        # Перевод команды в status=2 (возникла проблема):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
                elif command_name == 'get_contacts':
                    try:
                        await get_contacts(account_id, connection, metadata)
                        # Перевод команды в status=1 (выполнена):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                    except:
                        print(f'При выполнении команды {command_name} для аккаунта с id={account_id} возникли проблемы')
                        # Перевод команды в status=2 (возникла проблема):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
                elif command_name == 'get_dialogs':
                    try:
                        await get_dialogs(account_id, connection, metadata)
                        # Перевод команды в status=1 (выполнена):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                    except:
                        print(f'При выполнении команды {command_name} для аккаунта с id={account_id} возникли проблемы')
                        # Перевод команды в status=2 (возникла проблема):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
                elif command_name == 'get_big_files':
                    try:
                        await get_big_files(account_id, connection, metadata)
                        # Перевод команды в status=1 (выполнена):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                    except:
                        print(f'При выполнении команды {command_name} для аккаунта с id={account_id} возникли проблемы')
                        # Перевод команды в status=2 (возникла проблема):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
                elif command_name == 'send_message':
                    try:
                        send_message(account_id, command_args, connection, metadata)
                        await finish_send(account_id, connection, metadata)
                        # Перевод команды в status=1 (выполнена):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                    except:
                        print(f'При выполнении команды {command_name} для аккаунта с id={account_id} возникли проблемы')
                        # Перевод команды в status=2 (возникла проблема):
                        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
                # -------------
        print('Цикл поиска команд завершен')
        # --------------------------------------------------

        # Отслеживание новых сообщений для активных аккаунтов (status = 1 в таблице accounts из БД)
        # --------------------------------------------------
        accounts = Table('accounts', metadata)  # связываем переменную accounts с таблицей 'accounts' из БД
        query = select(accounts).where(accounts.c.status == 1)
        active_accounts = connection.execute(query)  # Создаем список активных аккаунтов, скаченных из БД
        if active_accounts:  # Если есть активные аккаунты, то для каждого проверяем наличие новых сообщений в Telegram:
            for account in active_accounts:
                print(f'Аккаунт с id={account[0]} и name = {account[1]} активен. Начинается проверка новых сообщений.')
                account_id = account[0]
                try:
                    await get_dialogs(account_id, connection, metadata)
                except:
                    print(f'При поиске новых сообщений для аккаунта с id={account_id} возникли проблемы')
        print('Цикл поиска новых сообщений для активных аккаунтов завершен')
        # --------------------------------------------------

if __name__=='__main__':
    asyncio.run(main())