from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
import logging
import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BOT_TOKEN, CHROME_OPTIONS, ADMIN_USERNAME
from src.scraper import ProductScraper
from src.report_generator import ReportGenerator
from src.user_manager import UserManager

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    filename='bot.log'
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

class ProductSearchBot:
    def __init__(self):
        self.chrome_options = Options()
        for option in CHROME_OPTIONS:
            self.chrome_options.add_argument(option)
        
        self.scraper = ProductScraper(self.chrome_options)
        self.report_generator = ReportGenerator()
        self.user_manager = UserManager()
        
        if not self.user_manager.is_admin(ADMIN_USERNAME):
            self.user_manager.allowed_users["admins"].append(ADMIN_USERNAME)
            self.user_manager._save_users()

    async def check_access(self, update: Update) -> bool:
        user = update.effective_user
        if not self.user_manager.is_allowed(username=user.username):
            await update.message.reply_text("У вас нет доступа к боту. Обратитесь к администратору.")
            return False
        return True

    async def welcome(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [[KeyboardButton("🔍 Начать поиск")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(WELCOME_MESSAGE, reply_markup=reply_markup)

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    async def admin_commands(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
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
            """
            await update.message.reply_text(help_text)

    async def search_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_access(update):
            return

        query = update.callback_query
        await query.answer()
        
        if query.data == 'search_okpd2':
            await query.message.reply_text("Введите код ОКПД2:")
            context.user_data['search_type'] = 'okpd2'
        elif query.data == 'search_name':
            await query.message.reply_text("Введите наименование продукции:")
            context.user_data['search_type'] = 'name'
        elif query.data == 'search_combined':
            await query.message.reply_text("Введите код ОКПД2 и наименование через запятую:")
            context.user_data['search_type'] = 'combined'

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self.check_access(update):
            return

        if update.message.text == "🔍 Начать поиск":
            await self.start(update, context)
            return

        if 'search_type' not in context.user_data:
            await update.message.reply_text("Пожалуйста, выберите тип поиска с помощью команды /start")
            return

        search_type = context.user_data['search_type']
        query = update.message.text

        await update.message.reply_text("🔍 Выполняется поиск...")

        try:
            if search_type == 'okpd2':
                results = self.scraper.search_gisp(okpd2=query)
            elif search_type == 'name':
                results = self.scraper.search_gisp(name=query)
            elif search_type == 'combined':
                okpd2, name = [x.strip() for x in query.split(',', 1)]
                results = self.scraper.search_gisp(okpd2=okpd2, name=name)

            if not results:
                await update.message.reply_text("По вашему запросу ничего не найдено.")
                return

            excel_report = self.report_generator.generate_excel_report(results)
            if excel_report:
                await update.message.reply_document(
                    document=excel_report,
                    filename='search_results.xlsx',
                    caption=f"Найдено результатов: {len(results)}"
                )
            else:
                await update.message.reply_text("Ошибка при формировании отчета.")

        except Exception as e:
            logger.error(f"Search error: {e}")
            await update.message.reply_text("Произошла ошибка при поиске. Попробуйте позже.")

        finally:
            context.user_data.pop('search_type', None)

    def run(self):
        try:
            application = Application.builder().token(BOT_TOKEN).build()
            
            application.add_handler(CommandHandler("start", self.welcome))
            application.add_handler(CommandHandler("help", self.welcome))
            application.add_handler(CommandHandler("admin", self.admin_commands))
            application.add_handler(CallbackQueryHandler(self.search_handler))
            application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
            
            application.run_polling()
        except Exception as e:
            logger.error(f"Bot error: {e}")

if __name__ == "__main__":
    bot = ProductSearchBot()
    bot.run()