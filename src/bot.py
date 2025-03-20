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

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    filename='bot.log',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

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
        user_id = update.effective_user.id
        logger.debug(f"Stop search for user {update.effective_user.username}")
        if user_id in self.active_searches:
            self.active_searches.remove(user_id)
            context.user_data.clear()
            await update.message.reply_text(
                "🛑 Поиск остановлен. Выберите тип нового поиска:", 
                reply_markup=ReplyKeyboardRemove()
            )
            await self.start(update, context)
        else:
            await update.message.reply_text("Нет активного поиска для остановки.")

    async def update_gisp(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        logger.debug(f"Manual GISP update requested by {user.username}")
        if not self.user_manager.is_admin(user.username):
            await update.message.reply_text("У вас нет прав администратора.")
            return

        status_message = await update.message.reply_text("⏳ Начало обновления файла ГИСП...")
        self.file_update_status = status_message

        try:
            await self.scraper.download_gisp_file_with_status(self.file_update_status)
            await status_message.edit_text("✅ Файл ГИСП успешно обновлен!")
        except Exception as e:
            logger.error(f"Manual GISP update error: {e}")
            await status_message.edit_text("❌ Ошибка при обновлении файла ГИСП")
        finally:
            self.file_update_status = None

    async def admin_commands(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        logger.debug(f"Admin command from {user.username}")
        if not self.user_manager.is_admin(user.username):
            await update.message.reply_text("У вас нет прав администратора.")
            return

        command = context.args[0] if context.args else "help"
        if command == "add":
            if len(context.args) < 2:
                await update.message.reply_text("Укажите username пользователя: /admin add username")
                return
            username = context.args[1]
            self.user_manager.add_user(username=username)
            await update.message.reply_text(f"Пользователь {username} добавлен")
        
        elif command == "remove":
            if len(context.args) < 2:
                await update.message.reply_text("Укажите username пользователя: /admin remove username")
                return
            username = context.args[1]
            self.user_manager.remove_user(username=username)
            await update.message.reply_text(f"Пользователь {username} удален")
        
        elif command == "list":
            users = self.user_manager.allowed_users["usernames"]
            message = "Список разрешенных пользователей:\n" + "\n".join(users)
            await update.message.reply_text(message)
        
        else:
            help_text = """
Команды администратора:
/admin add username - Добавить пользователя
/admin remove username - Удалить пользователя
/admin list - Показать список пользователей
/update_gisp - Обновить файл ГИСП
            """
            await update.message.reply_text(help_text)

    async def search_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_access(update):
            return

        query = update.callback_query
        await query.answer()
        
        if query.data.startswith('search_'):
            context.user_data['search_type'] = query.data.replace('search_', '')
            keyboard = [
                [InlineKeyboardButton(name, callback_data=f'source_{key}')]
                for key, name in SEARCH_SOURCES.items()
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text("Выберите источник поиска:", reply_markup=reply_markup)
        elif query.data.startswith('source_'):
            context.user_data['source'] = query.data.replace('source_', '')
            search_type = context.user_data['search_type']
            if search_type == 'okpd2':
                await query.message.reply_text("Введите код ОКПД2:")
            elif search_type == 'name':
                await query.message.reply_text("Введите наименование продукции:")
            elif search_type == 'combined':
                await query.message.reply_text("Введите код ОКПД2 и наименование через запятую:")

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
        query = update.message.text
        user_id = update.effective_user.id
        
        if user_id in self.active_searches:
            await update.message.reply_text("Поиск уже выполняется. Дождитесь результатов или остановите текущий поиск.")
            return
            
        self.active_searches.add(user_id)

        stop_keyboard = [[KeyboardButton("🛑 Остановить поиск")]]
        stop_markup = ReplyKeyboardMarkup(stop_keyboard, resize_keyboard=True)
        
        status_message = await update.message.reply_text(
            "🔍 Выполняется поиск...",
            reply_markup=stop_markup
        )

        try:
            if user_id not in self.active_searches:
                return

            source = context.user_data.get('source', 'all')
            results = []

            if search_type == 'okpd2':
                if source == 'gisp':
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ГИСП...")
                    results = self.scraper.search_gisp(okpd2=query)
                elif source == 'eaeu':
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ЕАЭС...")
                    results = self.scraper.search_eaeu(okpd2=query)
                else:
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ГИСП...")
                    gisp_results = self.scraper.search_gisp(okpd2=query)
                    if user_id not in self.active_searches:
                        return
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ЕАЭС...")
                    eaeu_results = self.scraper.search_eaeu(okpd2=query)
                    results = gisp_results + eaeu_results

            elif search_type == 'name':
                if source == 'gisp':
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ГИСП...")
                    results = self.scraper.search_gisp(name=query)
                elif source == 'eaeu':
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ЕАЭС...")
                    results = self.scraper.search_eaeu(name=query)
                else:
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ГИСП...")
                    gisp_results = self.scraper.search_gisp(name=query)
                    if user_id not in self.active_searches:
                        return
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ЕАЭС...")
                    eaeu_results = self.scraper.search_eaeu(name=query)
                    results = gisp_results + eaeu_results

            elif search_type == 'combined':
                try:
                    okpd2, name = [x.strip() for x in query.split(',', 1)]
                except ValueError:
                    await status_message.edit_text("❌ Неверный формат. Введите код ОКПД2 и наименование через запятую.")
                    return
                
                if source == 'gisp':
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ГИСП...")
                    results = self.scraper.search_gisp(okpd2=okpd2, name=name)
                elif source == 'eaeu':
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ЕАЭС...")
                    results = self.scraper.search_eaeu(okpd2=okpd2, name=name)
                else:
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ГИСП...")
                    gisp_results = self.scraper.search_gisp(okpd2=okpd2, name=name)
                    if user_id not in self.active_searches:
                        return
                    await status_message.edit_text("🔍 Выполняется поиск...\n⏳ Поиск в ЕАЭС...")
                    eaeu_results = self.scraper.search_eaeu(okpd2=okpd2, name=name)
                    results = gisp_results + eaeu_results

            if user_id not in self.active_searches:
                await status_message.delete()
                return

            if not results:
                await status_message.edit_text(
                    "❌ По вашему запросу ничего не найдено.",
                    reply_markup=ReplyKeyboardRemove()
                )
                await self.start(update, context)
                return

            await status_message.edit_text("📊 Формирование отчета...")
            excel_report = self.report_generator.generate_excel_report(results)
            
            if excel_report and user_id in self.active_searches:
                await status_message.delete()
                await update.message.reply_document(
                    document=excel_report,
                    filename='search_results.xlsx',
                    caption=f"✅ Найдено результатов: {len(results)}",
                    reply_markup=ReplyKeyboardRemove()
                )
                await self.start(update, context)
            else:
                await status_message.edit_text("❌ Ошибка при формировании отчета.")

        except Exception as e:
            logger.error(f"Search error: {e}")
            await status_message.edit_text("❌ Произошла ошибка при поиске. Попробуйте позже.")

        finally:
            if user_id in self.active_searches:
                self.active_searches.remove(user_id)
            context.user_data.clear()

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