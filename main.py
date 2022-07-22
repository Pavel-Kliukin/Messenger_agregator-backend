import os
import json
import asyncio
import mimetypes
from dotenv import load_dotenv
from datetime import datetime, timezone
from sqlalchemy import create_engine, MetaData, Table, select, update, insert, delete, and_, or_
from telethon import TelegramClient
from telethon.tl.types import User, Dialog, Channel, Chat, UserStatusOffline, UserStatusRecently, \
    UserStatusLastMonth, UserStatusLastWeek, PeerChannel, PeerChat, PeerUser, Message, MessageMediaUnsupported, \
    MessageMediaWebPage, MessageMediaPhoto, MessageMediaDocument, MessageMediaPoll, DocumentAttributeFilename
from telethon.utils import get_display_name
from telethon.errors import SessionPasswordNeededError, MediaInvalidError, ChannelPrivateError


# Начало авторизации пользователя в Телеграм
async def login_start(account_id, connection, metadata, command_id):
    accounts = Table('accounts', metadata)
    commands = Table('commands', metadata)
    client = await connect_to_telegram(account_id)
    try:
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=3))  # перевод аккаунта в status=3 (идет процесс логина)
        await client.connect()
        phone = '+' + connection.execute(select([accounts.c.login]).where(accounts.c.id == account_id)).fetchone()[0]
        ph = await client.send_code_request(phone, force_sms=True)
        phone_code_hash = ph.phone_code_hash  # может понадобиться для второй части авторизации
        # Записываем phone_code_hash в таблицу accounts:
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(phone_code_hash=phone_code_hash))

        logging(f'Ожидание кода Телеграма, из базы данных для аккаунта с id={account_id}')
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=4))  # перевод аккаунта в status=4 (ждёт код авторизации)
        # Перевод команды LOGIN в status=1 (команда выполнена):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
    except Exception as e:
        logging(f'При выполнении команды login (login_start) для аккаунта с id={account_id} возникли проблемы: \n{e}')

        # Перевод команды в status=2 (возникла проблема):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
    await client.disconnect()


# Завершение авторизации пользователя в Телеграм
async def login_finish(account_id, argument, connection, metadata, command_id, two_factor_verification=False):
    logging(f'Запуск функции login_finish для аккаунта с id={account_id}')
    accounts = Table('accounts', metadata)
    commands = Table('commands', metadata)
    client = await connect_to_telegram(account_id)
    try:
        code = json.loads(argument)['to_channel/code']
        phone = '+' + connection.execute(select([accounts.c.login]).where(accounts.c.id == account_id)).fetchone()[0]
        if two_factor_verification:
            # Занесение кода в колонку pass2fa таблицы accounts:
            connection.execute(update(accounts).where(accounts.c.id == account_id).values(pass2fa=code))
        else:
            # Занесение кода в колонку code таблицы accounts:
            connection.execute(update(accounts).where(accounts.c.id == account_id).values(code=code))

        await client.connect()
        try:
            logging(f'Оправка кода авторизации в Телеграм для аккаунта с id={account_id}')
            code = connection.execute(select([accounts.c.code]).where(accounts.c.id == account_id)).fetchone()[0]
            await client.sign_in(phone, code)  # Отправляем Телеграму код
        except SessionPasswordNeededError:  # Если стоит двухфакторная верификация
            # Перевод команды login_code либо login_2f в status=1 (команда выполнена):
            connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
            if two_factor_verification:
                code_2fa = connection.execute(select([accounts.c.pass2fa]).where(accounts.c.id == account_id)).fetchone()[0]
                await client.sign_in(password=code_2fa)
            else:
                # Перевод аккаунта в status=5 (ждёт код авторизации):
                connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=5))
                logging(f'У аккаунта с id={account_id} двухфакторная авторизация. Ожидание кода в поле code таблицы accounts...')
                return
        except ValueError:
            try:
                logging(f'Авторизация пользователя с id={account_id} производится с использованием phone_code_hash')
                phone_code_hash = connection.execute(select([accounts.c.phone_code_hash]).where(accounts.c.id == account_id)).fetchone()[0]
                await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
            except SessionPasswordNeededError:  # Если стоит двухфакторная верификация
                # Перевод команды login_code либо login_2f в status=1 (команда выполнена):
                connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
                if two_factor_verification:
                    code_2fa = connection.execute(select([accounts.c.pass2fa]).where(accounts.c.id == account_id)).fetchone()[0]
                    await client.sign_in(password=code_2fa)
                else:
                    # Перевод аккаунта в status=5 (ждёт код авторизации):
                    connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=5))
                    logging(f'У аккаунта с id={account_id} двухфакторная авторизация. Ожидание кода в поле code таблицы accounts...')
                    return

        if await client.is_user_authorized():
            logging(f'Авторизация в Телеграм пользователя с id={account_id} прошла успешно')
            # Перевод аккаунта в status=1 (аккаунт активен) и добавление времени активации в new_messages_last_check:
            connection.execute(update(accounts).where(
                accounts.c.id == account_id).values(status=1,
                                                    new_messages_last_check=datetime.now()))
            # Перевод команды login_code либо login_2f в status=1 (команда выполнена):
            connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
            # Удаление кода из колонки code таблицы accounts:
            connection.execute(update(accounts).where(accounts.c.id == account_id).values(code=None))
            # Удаление phone_code_hash из таблицы accounts:
            connection.execute(update(accounts).where(accounts.c.id == account_id).values(phone_code_hash=None))
        else:
            logging(f'НЕ УДАЛОСЬ авторизовать в Телеграм пользователя с id={account_id}')
            connection.execute(update(accounts).where(accounts.c.id == account_id).values(status=3))  # перевод аккаунта в status=3 (идет процесс логина)
    except Exception as e:
        logging(f'При выполнении функции login_finish для аккаунта с id={account_id} возникли проблемы\n {e}')
        # Перевод команды CODE в status=2 (при выполнении возникла ошибка):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
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
    accounts = Table('accounts', metadata)

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

        # Создание команды в БД на добавление записи в таблицу channels:
        query = insert(channels).values(
            account_id=account_id,
            channel=entity.id,
            name=get_display_name(entity) if entity_type is 'User' else entity.title,
            username=entity.username if entity_type not in ('Chat', 'ChatForbidden', 'ChatEmpty') else None,
            phone=entity.phone if entity_type is 'User' else None,
            type_channel=channel_type,
            cnt=entity.participants_count if entity_type in ('Chat', 'Channel') else None,
            can_view_participants=1 if entity_type in ('Chat', 'Channel') and entity.participants_count else 2,
            created_at=entity.date if entity_type in ('Chat', 'Channel') else None
        )
        connection.execute(query)  # отправление команды в БД
        # Обновление для аккаунта времени последнего взаимодействия с Телеграмом:
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(last_used_at=datetime.now()))


# Скачивание файла из сообщения и занесение информации в БД (большие файлы НЕ скачиваются, о них только заносится инф.)
async def file_download(message, dialog, client, account_id, connection, metadata):
    table_messages = Table('messages', metadata)
    message_files = Table('message_files', metadata)
    accounts = Table('accounts', metadata)

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

        if message.from_id:  # Определение from_id
            try:
                from_id = message.from_id.user_id
            except AttributeError:
                try:
                    from_id = message.from_id.channel_id
                except Exception as e:
                    logging(f'Что-то не так с from_id gри скачивании файла из сообщения. Ошибка: \n{e}', message)
                    from_id = None
        else:
            from_id = dialog.entity.id

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
            from_id=from_id,
            from_name=get_display_name(await client.get_entity(from_id)),
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

        # Обновление для аккаунта времени последнего взаимодействия с Телеграмом:
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(last_used_at=datetime.now()))


# Скачивание ОДНОЙ аватарки (с удалением старой, если она существовала)
async def avatar_download(account_id, entity, client, connection, metadata):
    channels = Table('channels', metadata)
    messenger_users = Table('messenger_users', metadata)
    accounts = Table('accounts', metadata)
    try:
        if entity.photo:
            avatar_path = f'avatars/{entity.id}.jpg'
            try:
                if os.path.isfile(avatar_path):  # Если аватарка уже была скачана ранее, то удаляем её
                    os.remove(avatar_path)
                await client.download_profile_photo(entity, f'avatars/{entity.id}')
            except PermissionError:
                logging('Не удалось удалить старую аватарку. Отказано в доступе.')

            # Внесение адреса аватарки в БД в таблицы channels и messenger_users
            # Если объект с таким аватаром есть в базе channels, то заносим в таблицу адрес аватарки
            if connection.execute(select(channels).where(channels.c.channel == entity.id)).fetchone():
                connection.execute(update(channels).where(channels.c.channel == entity.id).values(fn_avatar=f'avatars/{entity.id}.jpg'))
            # Если объект с таким аватаром есть в базе messenger_users, то заносим в таблицу адрес аватарки
            if connection.execute(select(messenger_users).where(messenger_users.c.peer_id == entity.id)).fetchone():
                connection.execute(update(messenger_users).where(messenger_users.c.peer_id == entity.id).values(fn_avatar=f'avatars/{entity.id}.jpg'))
    except AttributeError as e:
        logging(f'При скачивании аватарки возникли проблемы (AttributeError): \n{e}', entity)
    # Обновление для аккаунта времени последнего взаимодействия с Телеграмом:
    connection.execute(update(accounts).where(accounts.c.id == account_id).values(last_used_at=datetime.now()))


# Скачивание всех аватарок на сервер
async def get_avatars(account_id, connection, metadata, command_id):
    commands = Table('commands', metadata)
    # Перевод команды в status=3 (команда выполняется):
    connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))

    client = await connect_to_telegram(account_id)
    try:
        await client.connect()
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            await avatar_download(account_id, entity, client, connection, metadata)
        # Перевод команды в status=1 (команда выполнена):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
    except Exception as e:
        try:
            logging(f'При выполнении команды get_avatars для аккаунта с id={account_id} возникли проблемы: \n{e}', entity)
        except Exception:
            logging(f'При выполнении команды get_avatars для аккаунта с id={account_id} возникли проблемы: \n{e}')
        # Перевод команды в status=2 (возникла проблема):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
    await client.disconnect()


# Добавление ВСЕХ диалогов (с человеком или ботом), чатов и каналов пользователя в таблицу channels базы данных
async def get_all(account_id, connection, metadata, command_id):
    commands = Table('commands', metadata)
    # Перевод команды в status=3 (команда выполняется):
    connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))

    client = await connect_to_telegram(account_id)
    try:
        await client.connect()
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            await add_to_channels(entity, account_id, connection, metadata)
            await avatar_download(account_id, entity, client, connection, metadata)  # Скачивание (обновление) аватара
        # Перевод команды в status=1 (команда выполнена):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
    except Exception as e:
        try:
            logging(f'При выполнении команды get_all для аккаунта с id={account_id} возникли проблемы \n{e}', entity)
        except Exception:
            logging(f'При выполнении команды get_all для аккаунта с id={account_id} возникли проблемы \n{e}')
        # Перевод команды в status=2 (возникла проблема):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
    await client.disconnect()


# Добавление только контактов пользователя в БД в таблицу messenger_users
async def get_contacts(account_id, connection, metadata, command_id):
    commands = Table('commands', metadata)
    # Перевод команды в status=3 (команда выполняется):
    connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))
    client = await connect_to_telegram(account_id)
    try:
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

            # Создание команды в БД на добавление записи в таблицу messenger_users:
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

            await avatar_download(account_id, contact, client, connection, metadata)  # Скачивание (обновление) аватара:
        # Перевод команды в status=1 (выполнена):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
    except Exception as e:
        try:
            logging(f'При выполнении команды get_contacts для аккаунта с id={account_id} возникли проблемы: \n{e}', contact)
        except Exception:
            logging(f'При выполнении команды get_contacts для аккаунта с id={account_id} возникли проблемы: \n{e}')
        # Перевод команды в status=2 (возникла проблема):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
    await client.disconnect()


# Скачивание сообщений из всех диалогов в БД в таблицу messages
async def get_dialogs(account_id, connection, metadata, command_id=None):
    commands = Table('commands', metadata)
    messages_send = Table('messages_send', metadata)
    accounts = Table('accounts', metadata)
    table_messages = Table('messages', metadata)
    channels = Table('channels', metadata)

    if command_id:  # если функция get_dialogs была запущена командой get_dialogs, а не поиском новых сообщений, то:
        # Перевод команды в status=3 (выполняется):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))

    client = await connect_to_telegram(account_id)
    try:
        await client.connect()
        start_time = datetime.now()

        async for dialog in client.iter_dialogs():

            # Проверка занесен ли канал, из которого мы скачиваем сообщения, в БД в таблицу channels:
            channel_id = connection.execute(select([channels.c.id]).where(channels.c.channel == dialog.entity.id)).fetchone()
            if not channel_id:  # Если канала в БД нет, то добавляем его в таблицу channels
                logging(f'Канала с id={dialog.entity.id} нет в таблице channels. Добавляем его в таблицу.')
                await add_to_channels(dialog.entity, account_id, connection, metadata)
                await avatar_download(account_id, dialog.entity, client, connection, metadata)  # скачивание и внесение в БД аватара

            # Определяем, начиная с какой даты (времени) надо скачивать сообщения:
            if command_id:  # если функция запущена командой get_dialogs:
                last_check_date = datetime(1982, 11, 5)  # то берём очень старую дату (скачиваем ВСЕ сообщения)
            else:  # если функция запущена поиском новых сообщений:
                # то либо берём дату последнего обновления канала
                last_channels_upd = connection.execute(select([channels.c.lst_msgs_upd]).where(and_(
                    channels.c.channel == dialog.entity.id,
                    channels.c.account_id == account_id)
                )).fetchone()
                acc_activated_date = connection.execute(select([accounts.c.new_messages_last_check]).where(
                    accounts.c.id == account_id)).fetchone()
                # либо, если пользователь только что был активирован, то берём дату активации
                last_check_date = last_channels_upd[0] if last_channels_upd else acc_activated_date
            async for message in client.iter_messages(dialog):
                msg_date = message.date.replace(tzinfo=timezone.utc).astimezone(tz=None)
                if msg_date.timestamp() > last_check_date.timestamp():  # Если дата сообщения > даты последней проверки сообщений, то скачиваем его
                    # Создание команды в БД на добавление сообщения в таблицу messages:
                    if message.from_id:  # Определение from_id
                        try:
                            from_id = message.from_id.user_id
                        except AttributeError:
                            try:
                                from_id = message.from_id.channel_id
                            except Exception as e:
                                logging(f'Что-то не так с from_id из сообщения. Ошибка: \n{e}', message)
                                from_id = None
                    else:
                        from_id = dialog.entity.id

                    query = insert(table_messages).values(
                        bot_id=dialog.entity.id if isinstance(dialog.entity, User) and dialog.entity.bot else None,
                        account_id=account_id,
                        message_id=message.id,
                        channel_id_our=connection.execute(select([channels]).where(channels.c.channel == dialog.entity.id)).fetchone()[0],
                        channel_id=dialog.entity.id,
                        channel_name=connection.execute(select([channels]).where(channels.c.channel == dialog.entity.id)).fetchone()[4],
                        msg_date=msg_date,
                        napr=1 if message.out else 2,
                        from_id=from_id,
                        from_name=get_display_name(await client.get_entity(from_id)),
                        message=message.message,
                        src=json.dumps(message.to_dict(), default=str, ensure_ascii=False),
                        created_at=datetime.now()
                    )
                    connection.execute(query)  # Отправление команды в БД

                    #  Проверка, является ли сообщение отправленным из таблицы message_send:
                    #  Если является, то удаляем его из таблицы message_send
                    connection.execute(delete(messages_send).where(and_(
                        messages_send.c.id_of_telegram == message.id,
                        messages_send.c.channel_id == dialog.entity.id)))

                    # Если в сообщении есть файл, то скачиваем его
                    if message.media:
                        await file_download(message, dialog, client, account_id, connection, metadata)

                    entity = await client.get_entity(from_id)
                    await avatar_download(account_id, entity, client, connection, metadata)  # скачивание и внесение в БД аватара
                else:
                    break
            # Заносим новое время обновления сообщений диалога в таблицу channels
            connection.execute(update(channels).where(and_(
                channels.c.channel == dialog.entity.id,
                channels.c.account_id == account_id)
            ).values(lst_msgs_upd=datetime.now()))
            # Заносим время последнего сообщения (date_last_message) диалога в таблицу channels
            async for message in client.iter_messages(dialog):
                connection.execute(update(channels).where(and_(
                    channels.c.channel == dialog.entity.id,
                    channels.c.account_id == account_id)
                ).values(date_last_message=message.date.replace(tzinfo=timezone.utc).astimezone(tz=None)))
                break
        end_time = datetime.now()
        logging(f'Время проверки и скачивания сообщений для пользователя с id {account_id} составило {end_time - start_time}')
        # Добавление в таблицу accounts даты последней проверки новых сообщений:
        connection.execute(update(accounts).where(accounts.c.id == account_id).values(new_messages_last_check=end_time))
        if command_id:
            # Перевод команды в status=1 (выполнена):
            connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
    except ChannelPrivateError as e:
        logging(f'При выполнении команды get_dialogs либо поиске новых сообщений для аккаунта с id={account_id} возникли проблемы: \n{e}')
        if command_id:
            # Перевод команды в status=2 (возникла проблема):
            connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
    await client.disconnect()


# Скачивание БОЛЬШИХ файлов
async def get_big_files(account_id, connection, metadata, command_id):
    commands = Table('commands', metadata)
    # Перевод команды в status=3 (выполняется):
    connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))

    client = await connect_to_telegram(account_id)
    try:
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

        for entry in not_downloaded_files:

            # Скачивание файла на сервер
            dialog_id, message_id, file_name = entry[0], entry[1], entry[2]
            message = await client.get_messages(dialog_id, ids=message_id)
            path = f'storage/{dialog_id}/{message_id}'
            logging(f'Началась загрузка файла {file_name}')
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

            logging(f"Файл {file_name} скачен. Информация занесена в базу данных.")
        # Перевод команды в status=1 (выполнена):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
    except Exception as e:
        try:
            logging(f'При выполнении команды get_big_files для аккаунта с id={account_id} возникли проблемы \n{e}'
                    f'\nfile_name={file_name}'
                    f'\nmessage_id={message_id}'
                    f'\ndialog_id={dialog_id}')
        except Exception:
            logging(f'При выполнении команды get_big_files для аккаунта с id={account_id} возникли проблемы \n{e}')
        # Перевод команды в status=2 (возникла проблема):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
    await client.disconnect()


# Отправка сообщений:
async def send_message(account_id, arguments, connection, metadata, command_id, command_date):
    commands = Table('commands', metadata)
    messages_send = Table('messages_send', metadata)
    accounts = Table('accounts', metadata)
    # Перевод команды в status=3 (выполняется):
    connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))
    client = await connect_to_telegram(account_id)

    # Разбиваем сообщение на текст и файлы и вносим по одному в таблицу messages_send:
    try:
        arguments = json.loads(arguments)
        if arguments['message_text']:
            connection.execute(insert(messages_send).values(
                channel_id=str(arguments['to_channel/code']),
                date_msg=datetime.now(),
                from_account_id=account_id,
                message=arguments['message_text'],
                command_id=command_id,
                created_at=command_date
            ))
        if arguments['files']:
            files = arguments['files']
            for file in files:
                connection.execute(insert(messages_send).values(
                    channel_id=str(arguments['to_channel/code']),
                    date_msg=datetime.now(),
                    from_account_id=account_id,
                    file=file,
                    command_id=command_id,
                    created_at=command_date
                ))

        # Отправляем разбитое на текст и файлы сообщение и записываем телеграмные id-шники текста и файлов
        message_parts = connection.execute(select(
            messages_send.c.channel_id,
            messages_send.c.message,
            messages_send.c.file
        ).where(
            messages_send.c.command_id == command_id
        ).order_by(messages_send.c.id)).fetchall()

        await client.connect()

        to_channel = message_parts[0][0]
        text = message_parts[0][1]

        # Отправляемые файлы делим на 2 типа (медиа и остальные):
        media, documents = [], []
        for part in message_parts:
            file = part[2]
            if file:
                file_extension = os.path.splitext(file)[1].replace('.', '').lower()
                if file_extension in ('jpg', 'jpeg', 'png', 'mp4', 'mov'):
                    media.append(file)
                else:
                    documents.append(file)

        ids = []  # Список id-шников, которые Телеграм присвоит сообщению и прикрепленным к нему файлам
        if media or documents:
            if media:
                try:
                    # tg_answer нужен для получения id-шников отправляемых сообщения и файлов
                    tg_answer = await client.send_message(to_channel, text, file=media)
                    if type(tg_answer) is list:
                        for ta in tg_answer:
                            ids.append(ta.id)
                    else:
                        ids.append(tg_answer.id)

                    # Заносим id-шники присвоенные телеграмом в таблицу messages_send:
                    if text:
                        connection.execute(update(messages_send).where(
                            and_(
                                or_(
                                    messages_send.c.message == text,
                                    messages_send.c.file == media[0]
                                ),
                                messages_send.c.command_id == command_id
                            )).values(id_of_telegram=ids[0]))
                    else:
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == media[0],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[0]))
                    media.pop(0)
                    ids.pop(0)
                    for i in range(len(media)):
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == media[i],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[i]))

                except MediaInvalidError:  # Когда файлы не удается отправить одним пакетом, отправляем по одному
                    tg_answer = await client.send_message(to_channel, text)
                    ids.append(tg_answer.id)
                    for file in media:
                        tg_answer = await client.send_file(to_channel, file)
                        ids.append(tg_answer.id)
                        # Заносим id-шники присвоенные телеграмом в таблицу messages_send:
                    if text:
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.message == text,
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[0]))
                    else:
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == media[0],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[0]))
                    media.pop(0)
                    ids.pop(0)
                    for i in range(len(media)):
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == media[i],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[i]))

                if documents:
                    try:
                        tg_answer = await client.send_file(to_channel, file=documents)
                        # Заносим id-шники присвоенные телеграмом в таблицу messages_send:
                        if type(tg_answer) is list:
                            for i in range(len(tg_answer)):
                                connection.execute(update(messages_send).where(and_(
                                    messages_send.c.file == documents[i],
                                    messages_send.c.command_id == command_id
                                )).values(id_of_telegram=tg_answer[i].id))
                        else:
                            connection.execute(update(messages_send).where(and_(
                                messages_send.c.file == documents[0],
                                messages_send.c.command_id == command_id
                            )).values(id_of_telegram=tg_answer.id))
                    except MediaInvalidError:  # Когда файлы не удается отправить одним пакетом, отправляем по одному
                        for file in documents:
                            tg_answer = await client.send_file(to_channel, file)
                            # Заносим id-шники присвоенные телеграмом в таблицу messages_send:
                            connection.execute(update(messages_send).where(and_(
                                messages_send.c.file == file,
                                messages_send.c.command_id == command_id
                            )).values(id_of_telegram=tg_answer.id))
            else:
                try:
                    # tg_answer нужен для получения id-шников отправляемых сообщения и файлов
                    tg_answer = await client.send_message(to_channel, text, file=documents)
                    if type(tg_answer) is list:
                        for ta in tg_answer:
                            ids.append(ta.id)
                    else:
                        ids.append(tg_answer.id)

                    # Заносим id-шники присвоенные телеграмом в таблицу messages_send:
                    if text:
                        connection.execute(update(messages_send).where(
                            and_(
                                or_(
                                    messages_send.c.message == text,
                                    messages_send.c.file == documents[0]
                                ),
                                messages_send.c.command_id == command_id
                            )).values(id_of_telegram=ids[0]))
                    else:
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == documents[0],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[0]))
                    documents.pop(0)
                    ids.pop(0)
                    for i in range(len(documents)):
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == documents[i],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[i]))

                except MediaInvalidError:  # Когда файлы не удается отправить одним пакетом, отправляем по одному
                    tg_answer = await client.send_message(to_channel, text)
                    ids.append(tg_answer.id)
                    for file in documents:
                        tg_answer = await client.send_file(to_channel, file)
                        ids.append(tg_answer.id)
                        # Заносим id-шники присвоенные телеграмом в таблицу messages_send:
                    if text:
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.message == text,
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[0]))
                    else:
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == documents[0],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[0]))
                    documents.pop(0)
                    ids.pop(0)
                    for i in range(len(documents)):
                        connection.execute(update(messages_send).where(and_(
                            messages_send.c.file == documents[i],
                            messages_send.c.command_id == command_id
                        )).values(id_of_telegram=ids[i]))

        else:
            tg_answer = await client.send_message(to_channel, text)
            # Заносим id-шники присвоенные телеграмом в таблицу messages_send:
            connection.execute(update(messages_send).where(and_(
                messages_send.c.message == text,
                messages_send.c.command_id == command_id
            )).values(id_of_telegram=tg_answer.id))

        # Изменение поля sent_to_telegram отправленного сообщения и файлов из таблицы messages_send на значение = 1:
        connection.execute(update(messages_send).where(messages_send.c.command_id == command_id).values(sent_to_telegram=1))
        del ids
        logging(f'Сообщение созданное командой с id={command_id} отправлено')

        # Перевод команды в status=1 (выполнена):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=1))
    except Exception as e:
        logging(f'При выполнении команды отправки сообщения с id={command_id} возникли проблемы: \n{e}')
        # Перевод команды в status=2 (возникла проблема):
        connection.execute(update(commands).where(commands.c.id == command_id).values(status=2))
    # Обновление для аккаунта времени последнего взаимодействия с Телеграмом:
    connection.execute(update(accounts).where(accounts.c.id == account_id).values(last_used_at=datetime.now()))
    await client.disconnect()


def logging(text, entity=None):
    with open('our_logs.txt', 'a') as logs:
        logs.write(str(datetime.now())+'\n')
        logs.write(text+'\n')
        if entity:
            logs.write(str(entity))
        logs.write('\n')


async def main():
    load_dotenv('user.env')  # load_dotenv загружает из файла user.env переменные среды

    # Подключение к базе данных и настройка SQL Alchemy
    # --------------------------------------------------
    logging('Подключение к БД')
    host = os.environ.get('HOST')
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
        except Exception as e:
            logging(f'Не удалось получить данные из таблицы commands базы данных. Попытка повторится в следующем цикле. \n{e}')
        if commands_list:  # Если список не пустой (в БД были команды), то отправляем команды на выполнение:
            for command in commands_list:
                command_id = command[0]
                command_name = command[1]
                account_id = command[2]
                command_args = command[3]
                command_date = command[4]
                # Перевод команды в status=3 (в процессе выполнения):
                connection.execute(update(commands).where(commands.c.id == command_id).values(status=3))
                # -------------
                if command_name == 'login_start':
                    logging(f'Запуск функции login_start для аккаунта с id={account_id}')
                    await login_start(account_id, connection, metadata, command_id)
                elif command_name == 'login_code':
                    await login_finish(account_id, command_args, connection, metadata, command_id)
                elif command_name == 'login_2f':
                    await login_finish(account_id, command_args, connection, metadata, command_id, two_factor_verification=True)
                elif command_name == 'get_avatars':
                    logging(f'Запущена процедура скачивания всех аватарок для аккаунта с id={account_id}')
                    await get_avatars(account_id, connection, metadata, command_id)
                elif command_name == 'get_all':
                    logging(f'Запущена процедура скачивания всех чатов (get_all) для аккаунта с id={account_id}')
                    await get_all(account_id, connection, metadata, command_id)
                elif command_name == 'get_contacts':
                    logging(f'Запущена процедура скачивания всех контактов (get_contacts) для аккаунта с id={account_id}')
                    await get_contacts(account_id, connection, metadata, command_id)
                elif command_name == 'get_dialogs':
                    logging(f'Запущена процедура скачивания всех сообщений (get_dialogs) для аккаунта с id={account_id}')
                    await get_dialogs(account_id, connection, metadata, command_id)
                elif command_name == 'get_big_files':
                    logging(f'Запущена процедура скачивания больших файлов для аккаунта с id={account_id}')
                    await get_big_files(account_id, connection, metadata, command_id)
                elif command_name == 'send_message':
                    logging(f'Запущена процедура отправки сообщений для аккаунта с id={account_id}')
                    await send_message(account_id, command_args, connection, metadata, command_id, command_date)
                # -------------
        # --------------------------------------------------

        # Отслеживание новых сообщений для активных аккаунтов (status = 1 в таблице accounts из БД)
        # --------------------------------------------------
        accounts = Table('accounts', metadata)  # связываем переменную accounts с таблицей 'accounts' из БД
        query = select(accounts).where(accounts.c.status == 1)
        active_accounts = connection.execute(query)  # Создаем список активных аккаунтов, скаченных из БД
        if active_accounts:  # Если есть активные аккаунты, то для каждого проверяем наличие новых сообщений в Telegram:
            for account in active_accounts:
                logging(f'Аккаунт с id={account[0]} и name = {account[1]} активен. Начинается проверка новых сообщений.')
                account_id = account[0]
                await get_dialogs(account_id, connection, metadata)
        # --------------------------------------------------

if __name__=='__main__':
    asyncio.run(main())