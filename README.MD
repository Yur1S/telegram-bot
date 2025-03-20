```markdown:c:\Users\yur1s\Documents\GitHub\telegram-bot\README.md
# Telegram Bot для поиска продукции 🔍

Бот для поиска продукции в базах данных ГИСП и ЕАЭС с возможностью выгрузки результатов в Excel.

## Возможности

- 🔍 Поиск по коду ОКПД2
- 📝 Поиск по наименованию продукции
- 🔄 Комбинированный поиск (ОКПД2 + наименование)
- 📊 Выгрузка результатов в Excel
- 👥 Система управления доступом пользователей
- 🔄 Ежедневное автоматическое обновление базы ГИСП
- 🌐 Интеграция с API ЕАЭС

## Требования

- Python 3.8+
- Токен Telegram бота (получить у [@BotFather](https://t.me/BotFather))
- 512MB RAM минимум

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/your-username/telegram-bot.git
cd telegram-bot
```

2. Создайте виртуальное окружение:
```bash
python -m venv venv
venv\Scripts\activate
```

3. Установите зависимости:
```bash
pip install -r requirements.txt
```

4. Создайте файл config.py:
```python
BOT_TOKEN = 'ваш_токен_бота'
ADMIN_USERNAME = 'ваш_username_в_telegram'
```

## Запуск

### Локальный запуск
```bash
python src/bot.py
```

### Запуск на сервере

1. Создайте службу:
```bash
nano /etc/systemd/system/telegram-bot.service
```

2. Содержимое службы:
```ini
[Unit]
Description=Telegram Search Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/bots/telegram-bot
Environment=PYTHONPATH=/root/bots/telegram-bot
Environment=VIRTUAL_ENV=/root/bots/telegram-bot/venv
Environment=PATH=/root/bots/telegram-bot/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin
ExecStart=/root/bots/telegram-bot/venv/bin/python3 -u /root/bots/telegram-bot/src/bot.py
Restart=always
RestartSec=10
MemoryLimit=450M

[Install]
WantedBy=multi-user.target
```

3. Запустите службу:
```bash
systemctl enable telegram-bot
systemctl start telegram-bot
```

## Использование

### Команды бота

- `/start` - Начать работу с ботом
- `/help` - Показать справку
- `/stop` - Остановить текущий поиск

### Команды администратора

- `/admin add username` - Добавить пользователя
- `/admin remove username` - Удалить пользователя
- `/admin list` - Показать список пользователей

## Мониторинг

### Просмотр логов
```bash
journalctl -u telegram-bot -f
```

### Статус службы
```bash
systemctl status telegram-bot
```

## Обслуживание

### Перезапуск бота
```bash
systemctl restart telegram-bot
```

### Обновление зависимостей
```bash
cd /root/bots/telegram-bot
source venv/bin/activate
pip install -r requirements.txt --upgrade
systemctl restart telegram-bot
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
│   └── gisp_products.xlsx
├── config.py
├── requirements.txt
└── README.md
```

## Решение проблем

1. Бот не отвечает:
   - Проверьте статус службы
   - Проверьте логи
   - Убедитесь, что токен бота указан правильно

2. Ошибки при поиске:
   - Проверьте наличие файла ГИСП в директории data
   - Проверьте доступность API ЕАЭС
   - Проверьте формат запроса

3. Проблемы с памятью:
   - Проверьте настройки MemoryLimit в службе
   - Мониторьте использование памяти
   - При необходимости увеличьте лимит памяти

## Лицензия

MIT
```