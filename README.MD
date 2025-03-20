```markdown:c:\Users\yur1s\Documents\GitHub\telegram-bot\README.md
# Telegram Bot для поиска продукции в ГИСП и ЕАЭС

## Создание и настройка бота в Telegram

1. Создание бота:
   - Откройте Telegram
   - Найдите @BotFather
   - Отправьте команду `/newbot`
   - Введите имя бота (например "Product Search Bot")
   - Введите username бота (должен заканчиваться на bot)
   - Сохраните полученный токен бота (BOT_TOKEN)

2. Настройка бота через BotFather:
   - `/setdescription` - установите описание
   - `/setabouttext` - установите краткую информацию
   - `/setuserpic` - установите аватар
   - `/setcommands` - добавьте команды:
     ```
     start - Начать поиск
     help - Показать справку
     stop - Остановить поиск
     admin - Команды администратора
     update_gisp - Обновить базу ГИСП
     ```

## Установка на сервер

### 1. Подготовка сервера
```bash
# Создаем директорию для бота
mkdir -p /root/bots
cd /root/bots

# Клонируем репозиторий
git clone [URL репозитория] telegram-bot
cd telegram-bot

# Устанавливаем необходимые пакеты
apt update
apt install python3-venv python3-full git

# Создаем виртуальное окружение
python3 -m venv venv

# Активируем виртуальное окружение
source venv/bin/activate

# Устанавливаем зависимости
pip install python-telegram-bot pandas requests xlsxwriter schedule
```

### 2. Настройка конфигурации
Создайте файл config.py:
```bash
nano config.py
```

Вставьте в него:
```python
BOT_TOKEN = "ваш_токен_бота"  # Токен от @BotFather
ADMIN_USERNAME = "ваш_username"  # Ваш username в Telegram без @
```

### 3. Создание структуры директорий
```bash
# Создаем необходимые директории
mkdir -p data
mkdir -p src

# Проверяем права доступа
chmod 755 -R /root/bots/telegram-bot
```

### 4. Настройка системного сервиса
```bash
nano /etc/systemd/system/telegram-bot.service
```

Вставьте:
```ini
[Unit]
Description=Telegram Bot Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/bots/telegram-bot
ExecStart=/root/bots/telegram-bot/venv/bin/python src/bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 5. Запуск и проверка
```bash
# Перезагружаем демона
systemctl daemon-reload

# Включаем автозапуск
systemctl enable telegram-bot

# Запускаем бота
systemctl start telegram-bot

# Проверяем статус
systemctl status telegram-bot
```

## Первичная настройка бота

1. Откройте бота в Telegram
2. Отправьте команду `/start`
3. Если вы указали свой username в config.py как ADMIN_USERNAME, вы автоматически получите права администратора
4. Добавьте дополнительных пользователей:
   ```
   /admin add username1
   /admin add username2
   ```
5. Проверьте список пользователей:
   ```
   /admin list
   ```
6. Обновите базу ГИСП:
   ```
   /update_gisp
   ```

## Управление ботом

### Команды в Telegram
- `/start` - Начать поиск
- `/help` - Показать справку
- `/stop` - Остановить поиск

### Команды администратора
- `/admin add username` - Добавить пользователя
- `/admin remove username` - Удалить пользователя
- `/admin list` - Список пользователей
- `/update_gisp` - Обновить базу ГИСП

### Управление сервисом
```bash
# Перезапустить бота
systemctl restart telegram-bot

# Остановить бота
systemctl stop telegram-bot

# Запустить бота
systemctl start telegram-bot

# Проверить статус
systemctl status telegram-bot

# Посмотреть логи
journalctl -u telegram-bot -f
```

## Устранение проблем

### Бот не отвечает
1. Проверьте статус:
```bash
systemctl status telegram-bot
```

2. Проверьте логи:
```bash
journalctl -u telegram-bot -f
tail -f /root/bots/telegram-bot/bot.log
```

3. Проверьте конфигурацию:
```bash
cat config.py
cat data/users.json
```

### Ошибки доступа
1. Проверьте права:
```bash
ls -la /root/bots/telegram-bot
ls -la /root/bots/telegram-bot/data
```

2. Исправьте права если нужно:
```bash
chmod 755 -R /root/bots/telegram-bot
chmod 644 config.py
chmod 644 data/users.json
```

### Обновление бота
```bash
cd /root/bots/telegram-bot
git pull
source venv/bin/activate
pip install -r requirements.txt
systemctl restart telegram-bot
```

## Резервное копирование
```bash
# Копирование конфигурации
cp config.py config.py.backup

# Копирование базы пользователей
cp data/users.json data/users.json.backup

# Архивация всех данных
tar -czf backup.tar.gz config.py data/users.json
```

## Мониторинг и обслуживание

### Очистка логов
```bash
# Очистка лога бота
truncate -s 0 bot.log

# Очистка системных логов (оставить логи за последние 7 дней)
journalctl --vacuum-time=7d
```

### Проверка использования ресурсов
```bash
# Использование CPU и памяти
top -p $(pgrep -f "python src/bot.py")

# Использование диска
du -sh /root/bots/telegram-bot
```

## Структура проекта
```
telegram-bot/
├── src/
│   ├── bot.py
│   ├── scraper.py
│   ├── report_generator.py
│   └── user_manager.py
├── data/
│   ├── users.json
│   └── gisp_products.csv
├── config.py
├── bot.log
└── README.md
```

## Важные замечания
- Убедитесь, что сервер имеет стабильное подключение к интернету
- Регулярно проверяйте логи на наличие ошибок
- Делайте резервные копии конфигурации и базы пользователей
- При обновлении сохраняйте файл config.py
- Следите за свободным местом на диске
```