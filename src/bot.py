from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
import pandas as pd
import logging
import asyncio
import os
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BOT_TOKEN, ADMIN_USERNAME
from src.scraper import ProductScraper
from src.report_generator import ReportGenerator
from src.user_manager import UserManager

# Убираем существующую настройку логирования
# logging.basicConfig(...)

# Настраиваем логирование
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Создаем обработчик для файла
file_handler = logging.FileHandler('bot.log', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Создаем обработчик для консоли
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(levelname)s: %(message)s')
console_handler.setFormatter(console_formatter)

# Добавляем оба обработчика
logger.addHandler(file_handler)
logger.addHandler(console_handler)

WELCOME_MESSAGE = """
👋 Добро пожаловать в бот для поиска продукции!

Этот бот поможет вам найти информацию о продукции в базах данных ГИСП и ЕАЭС.

Для начала работы нажмите кнопку "🔍 Начать поиск" или используйте команду /start

Доступные команды:
/start - Начать поиск
/help - Показать справку
"""

HELP_MESSAGE = """
📖 Справка по использованию бота

Основные команды:
/start - Начать новый поиск
/help - Показать это сообщение
/stop - Остановить текущий поиск

Типы поиска:
1. 🔍 Поиск по ОКПД2
   - Введите код ОКПД2 (например: 26.20.11)

2. 📝 Поиск по наименованию
   - Введите название продукции (например: компьютер)

3. 🔄 Комбинированный поиск
   - Введите код ОКПД2 и название через запятую
   - Пример: 26.20.11, компьютер

Источники поиска:
- 🌐 Везде (ГИСП + ЕАЭС)
- 📊 ГИСП
- 🔄 ЕАЭС

Для администраторов:
/admin add username - Добавить пользователя
/admin remove username - Удалить пользователя
/admin list - Список пользователей
/update_gisp - Обновление файла ГИСП
"""

SEARCH_SOURCES = {
    'all': 'Везде',
    'gisp': 'ГИСП',
    'eaeu': 'ЕАЭС'
}


class ProductSearchBot:
    def __init__(self):
        logger.debug("Initializing ProductSearchBot...")
        try:
            self.scraper = ProductScraper()
            self.report_generator = ReportGenerator()
            self.user_manager = UserManager()
            self.active_searches = set()
            self.file_update_status = None
            
            # Проверяем и создаем директорию для данных
            os.makedirs('data', exist_ok=True)
            
            # Инициализируем файл пользователей, если он не существует
            if not os.path.exists('data/users.json'):
                with open('data/users.json', 'w', encoding='utf-8') as f:
                    json.dump({"admins": [ADMIN_USERNAME], "usernames": []}, f)
            
            if not self.user_manager.is_admin(ADMIN_USERNAME):
                logger.debug(f"Adding {ADMIN_USERNAME} as admin")
                self.user_manager.allowed_users["admins"].append(ADMIN_USERNAME)
                self.user_manager._save_users()
            logger.debug("ProductSearchBot initialized successfully")
        except Exception as e:
            logger.error(f"Error during initialization: {e}", exc_info=True)
            raise

    async def check_access(self, update: Update) -> bool:
        user = update.effective_user
        has_access = self.user_manager.is_allowed(username=user.username)
        logger.debug(f"Access check for {user.username}: {has_access}")
        if not has_access:
            await update.message.reply_text("У вас нет доступа к боту. Обратитесь к администратору.")
            return False
        return True

    async def welcome(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            logger.debug(f"Welcome message for user {update.effective_user.username}")
            keyboard = [[KeyboardButton("🔍 Начать поиск")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                WELCOME_MESSAGE, 
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            logger.debug("Welcome message sent successfully")
        except Exception as e:
            logger.error(f"Error in welcome handler: {e}", exc_info=True)

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_access(update):
            return
        await update.message.reply_text(HELP_MESSAGE)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            logger.debug(f"Start command from user {update.effective_user.username}")
            if not await self.check_access(update):
                return

            keyboard = [
                [
                    InlineKeyboardButton("Поиск по ОКПД2", callback_data='search_okpd2'),
                    InlineKeyboardButton("Поиск по наименованию", callback_data='search_name')
                ],
                [InlineKeyboardButton("Комбинированный поиск", callback_data='search_combined')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "Выберите тип поиска:",
                reply_markup=reply_markup
            )
            logger.debug("Start command processed successfully")
        except Exception as e:
            logger.error(f"Error in start handler: {e}", exc_info=True)

    async def stop_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            user_id = update.effective_user.id
            logger.debug(f"Stop search requested for user {update.effective_user.username}")
            
            if user_id in self.active_searches:
                self.active_searches.remove(user_id)
                context.user_data.clear()
                
                # Удаляем клавиатуру и отправляем сообщение
                reply_markup = ReplyKeyboardRemove()
                await update.message.reply_text(
                    "🛑 Поиск остановлен",
                    reply_markup=reply_markup
                )
                # Возвращаемся к начальному меню
                await self.start(update, context)
            else:
                await update.message.reply_text(
                    "❌ Нет активного поиска для остановки\n"
                    "Используйте /start для начала нового поиска"
                )
        except Exception as e:
            logger.error(f"Error in stop_search: {e}")
            await update.message.reply_text("❌ Произошла ошибка при остановке поиска")

    async def update_gisp(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        logger.debug(f"Manual GISP update requested by {user.username}")
        if not self.user_manager.is_admin(user.username):
            await update.message.reply_text("У вас нет прав администратора.")
            return

        status_message = await update.message.reply_text("⏳ Начало обновления файла ГИСП...")
        self.file_update_status = status_message

        try:
            logger.debug("Starting GISP file download process...")
            await status_message.edit_text("⏳ Загрузка файла ГИСП...")
            
            total_rows = await self.scraper.download_gisp_file_with_status(status_message)
            
            # Проверяем результаты обновления
            if not os.path.exists(self.scraper.GISP_FILE_PATH):
                raise Exception("CSV файл не был создан")
                
            if os.path.getsize(self.scraper.GISP_FILE_PATH) == 0:
                raise Exception("CSV файл создан, но пуст")
                
            if total_rows <= 0:
                raise Exception("Не было обработано ни одной строки")
            
            # Проверяем и удаляем временный файл
            temp_file = self.scraper.TEMP_GISP_FILE
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    logger.info("Temporary Excel file removed successfully")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary file: {e}")
            
            logger.debug(f"GISP file download completed, processed {total_rows} rows")
            await status_message.edit_text(
                f"✅ Файл ГИСП успешно обновлен!\n"
                f"📊 Обработано строк: {total_rows:,}\n"
                f"📁 Размер файла: {os.path.getsize(self.scraper.GISP_FILE_PATH) / (1024*1024):.1f} MB"
            )
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Manual GISP update error: {error_msg}", exc_info=True)
            await status_message.edit_text(
                f"❌ Ошибка при обновлении файла ГИСП:\n"
                f"{error_msg[:200]}\n\n"
                "Проверьте логи для получения дополнительной информации."
            )
        finally:
            self.file_update_status = None

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_access(update):
            return

        if update.message.text == "🔍 Начать поиск":
            await self.start(update, context)
            return
        
        if update.message.text == "🛑 Остановить поиск":
            await self.stop_search(update, context)
            return

        if 'search_type' not in context.user_data:
            await update.message.reply_text("Пожалуйста, выберите тип поиска с помощью команды /start")
            return

        search_type = context.user_data['search_type']
        source = context.user_data.get('source', 'all')
        query = update.message.text.strip()
        user_id = update.effective_user.id
        
        if user_id in self.active_searches:
            await update.message.reply_text("🔄 Поиск уже выполняется. Дождитесь результатов или остановите текущий поиск.")
            return
            
        self.active_searches.add(user_id)
        
        try:
            status_message = await update.message.reply_text("⏳ Начинаем поиск...")
            
            if search_type == 'okpd2':
                results = await self.scraper.search_all(okpd2=query, status_message=status_message)
            elif search_type == 'name':
                results = await self.scraper.search_all(name=query, status_message=status_message)
            elif search_type == 'combined':
                try:
                    okpd2, name = [x.strip() for x in query.split(',', 1)]
                    results = await self.scraper.search_all(okpd2=okpd2, name=name, status_message=status_message)
                except ValueError:
                    await status_message.edit_text("❌ Неверный формат. Введите код ОКПД2 и наименование через запятую")
                    self.active_searches.remove(user_id)
                    return
            
            if not results:
                await status_message.edit_text("❌ Ничего не найдено")
                self.active_searches.remove(user_id)
                return
                
            # Отправляем результаты частями
            chunk_size = 10
            for i in range(0, len(results), chunk_size):
                chunk = results[i:i + chunk_size]
                message = f"📄 Результаты поиска (часть {i//chunk_size + 1}/{-(-len(results)//chunk_size)}):\n\n"
                for item in chunk:
                    message += (
                        f"🏢 {item['manufacturer']}\n"
                        f"📦 {item['name']}\n"
                        f"📝 ОКПД2: {item['okpd2_code']}\n"
                        f"🔢 ИНН: {item['inn']}\n"
                        f"📋 Реестровый номер: {item['registry_number']}\n"
                        f"📅 Дата регистрации: {item['registry_date']}\n"
                        f"⏳ Действует до: {item['valid_until']}\n"
                        f"🌐 Источник: {item['source']}\n"
                        f"{'=' * 30}\n"
                    )
                await update.message.reply_text(message)
                
        except Exception as e:
            logger.error(f"Search error: {e}", exc_info=True)
            await status_message.edit_text(f"❌ Ошибка при поиске: {str(e)}")
        finally:
            self.active_searches.remove(user_id)

    # Add this method to your ProductSearchBot class
    async def admin_commands(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin commands"""
        if not await self.check_access(update):
            return
            
        user = update.effective_user
        if not self.user_manager.is_admin(user.username):
            await update.message.reply_text("У вас нет прав администратора.")
            return
            
        try:
            command_parts = update.message.text.split()
            if len(command_parts) < 2:
                await update.message.reply_text(
                    "Доступные команды:\n"
                    "/admin add username - Добавить пользователя\n"
                    "/admin remove username - Удалить пользователя\n"
                    "/admin list - Список пользователей"
                )
                return
                
            action = command_parts[1].lower()
            
            if action == "list":
                users = self.user_manager.get_all_users()
                admins = users.get("admins", [])
                regular_users = users.get("usernames", [])
                
                message = "📊 Список пользователей:\n\n"
                message += "👑 Администраторы:\n"
                for admin in admins:
                    message += f"- {admin}\n"
                    
                message += "\n👤 Пользователи:\n"
                for user in regular_users:
                    message += f"- {user}\n"
                    
                await update.message.reply_text(message)
                
            elif action in ["add", "remove"] and len(command_parts) == 3:
                target_username = command_parts[2]
                if action == "add":
                    self.user_manager.add_user(target_username)
                    await update.message.reply_text(f"✅ Пользователь {target_username} добавлен")
                else:
                    self.user_manager.remove_user(target_username)
                    await update.message.reply_text(f"❌ Пользователь {target_username} удален")
            else:
                await update.message.reply_text("❌ Неверный формат команды")
                
        except Exception as e:
            logger.error(f"Admin command error: {e}", exc_info=True)
            await update.message.reply_text(f"❌ Ошибка: {str(e)}")

    def run(self):
        try:
            logger.info("Starting bot application...")
            application = Application.builder().token(BOT_TOKEN).build()
            
            application.add_handler(CommandHandler("start", self.welcome))
            application.add_handler(CommandHandler("help", self.help))
            application.add_handler(CommandHandler("stop", self.stop_search))
            application.add_handler(CommandHandler("admin", self.admin_commands))
            application.add_handler(CommandHandler("update_gisp", self.update_gisp))
            application.add_handler(CallbackQueryHandler(self.search_handler))
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
            
            logger.info("Starting polling...")
            application.run_polling()
        except Exception as e:
            logger.error(f"Bot error: {e}")

if __name__ == "__main__":
    logger.info("Main program starting...")
    bot = ProductSearchBot()
    bot.run()