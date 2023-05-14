## Для запуска бота требуется:
- Перейти в папку со скриптом
- В консоли ввести:
  - `python3 main.py`

## Чтобы воспользоваться командами нужно:
  - **Либо** "вручную" внести команду и id аккаунта в таблицу ***commands*** базы данных ( работает для всех команд, кроме `send_message`, потому что там надо ещё в формате *json* вносить текст сообщения и вложенные файлы ),   
  - **Либо** открыть новую сессию консоли и в ней прописать:
    - `python3 commands.py <command> <account_id>` (Вместо *< command >* ввести одну из предложенных ниже команд, вместо *< account_id >* ввести id аккаунта, для которого должна быть выполнена команда. ID аккаунтов можно посмотреть в базе данных, в таблице ***accounts***)

## Список доступных команд:
`login_start` - Запускает авторизацию нового аккаунта в Телеграме (отправляет в Телеграм запрос на код авторизации)  
`login_code` - выглядит так: `python3 commands.py login_code <account_id> <код> `. После получения от Телеграма кода, завершает процесс авторизации  
`login_2f` - выглядит так: `python3 commands.py login_2f <account_id> <код 2-х факторной авторизации> `. После получения от Телеграма кода, завершает процесс авторизации  
`get_avatars` - скачивает аватарки всех каналов и добавляет в базу данных  
`get_all` - Собирает данные о всех чатах и добавляет в базу данных(таблица ***channels***)  
`get_contacts` - Собирает данные только о ваших контактах и добавляет в базу данных(таблица ***messenger_users***)  
`get_dialogs` - Собирает все сообщения из всех диалогов и добавляет в базу данных(таблица ***messages***)  
`get_big_files` - Скачивает все большие файлы из сообщений для указанного аккаунта, которые не были скачены во время выполнения команды `get_dialogs` или мониторинга новых сообщений  
`send_message` - Отправить сообщение. Сначала оно кладется в таблицу ***messages_send***. Потом отправляется. И после обнаружения его среди новых сообщений Телеграма, удаляется из таблицы.  
Команда выглядит таким образом:  
`python3 commands.py send_message <account_id> <chat_id> -m <текст сообщения> -f file1 file2 file3`  
Вместо *< chat_id >* надо указать id пользователя, чата или канала в Телеграм, которому вы хотите отправить сообщение. Текст сообщения, а также каждый путь к файлу надо указывать без кавычек.  


#### Также, бот в фоновом режиме собирает новые поступившие сообщения и добавляет информацию о них в базу данных (таблица messages)
