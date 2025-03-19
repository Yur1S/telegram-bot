```markdown:c:\Users\yur1s\Projects\telegram-bot\README.md
# Telegram Search Bot

## Описание
Telegram бот для поиска продукции в базах данных ГИСП и ЕАЭС. Бот позволяет осуществлять поиск по коду ОКПД2 и наименованию продукции как по отдельности, так и комбинированно.

## Установка и настройка

### Предварительные требования
- Python 3.8 или выше
- Git
- Токен Telegram бота (получить у [@BotFather](https://t.me/BotFather))

### Локальная установка
1. Клонируйте репозиторий:
```bash
git clone https://github.com/ваш-username/telegram-bot.git
cd c:\Users\yur1s\Projects\telegram-bot
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

4. Настройте конфигурацию:
- Откройте файл `config.py`
- Замените `YOUR_BOT_TOKEN` на ваш токен бота
- Замените `YOUR_ADMIN_USERNAME` на ваш username в Telegram

5. Запустите бота:
```bash
python src\bot.py
```

### Установка на VPS

1. Подключитесь к VPS:
```bash
ssh username@your_vps_ip
```

2. Установите необходимые пакеты:
```bash
sudo apt update
sudo apt install -y git python3-pip python3-venv chromium-browser chromium-chromedriver
```

3. Клонируйте и настройте проект:
```bash
mkdir ~/bots
cd ~/bots
git clone https://github.com/ваш-username/telegram-bot.git
cd telegram-bot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

4. Создайте службу systemd:
```bash
sudo nano /etc/systemd/system/telegram-bot.service
```

Добавьте следующее содержимое:
```plaintext
[Unit]
Description=Telegram Search Bot
After=network.target

[Service]
Type=simple
User=ваш_username
WorkingDirectory=/home/ваш_username/bots/telegram-bot
Environment=PYTHONPATH=/home/ваш_username/bots/telegram-bot
ExecStart=/home/ваш_username/bots/telegram-bot/venv/bin/python src/bot.py
Restart=always

[Install]
WantedBy=multi-user.target
```

5. Настройте права и запустите бота:
```bash
chmod +x start.sh
sudo systemctl daemon-reload
sudo systemctl enable telegram-bot
sudo systemctl start telegram-bot
```

## Использование

### Команды администратора
- `/start` - Начать работу с ботом
- `/admin add username` - Добавить пользователя
- `/admin remove username` - Удалить пользователя
- `/admin list` - Показать список пользователей

### Поиск
1. Выберите тип поиска:
   - Поиск по ОКПД2
   - Поиск по наименованию
   - Комбинированный поиск
2. Введите параметры поиска
3. Получите результаты в виде сообщения или файла

## Обслуживание

### Просмотр логов
```bash
sudo journalctl -u telegram-bot -f
```

### Перезапуск бота
```bash
sudo systemctl restart telegram-bot
```

### Обновление бота
```bash
cd ~/bots/telegram-bot
git pull
sudo systemctl restart telegram-bot
```

## Устранение неполадок

1. Проверьте статус службы:
```bash
sudo systemctl status telegram-bot
```

2. Проверьте логи на наличие ошибок:
```bash
sudo journalctl -u telegram-bot -f
```

3. Убедитесь, что:
   - Токен бота указан правильно
   - Файл `allowed_users.json` существует и имеет правильные права
   - Все зависимости установлены корректно
   - Сервис запущен и работает

## Безопасность
- Регулярно обновляйте систему
- Делайте резервные копии файла `allowed_users.json`
- Используйте только защищенное SSH-соединение
- Регулярно проверяйте логи на подозрительную активность
```