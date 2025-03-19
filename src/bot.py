from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
import logging
import asyncio
import os
from config import BOT_TOKEN, CHROME_OPTIONS, ADMIN_USERNAME
from .scraper import ProductScraper
from .report_generator import ReportGenerator
from .user_manager import UserManager

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    filename='bot.log'
)
logger = logging.getLogger(__name__)

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
            "Добро пожаловать! Выберите тип поиска:",
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

    def run(self):
        try:
            application = Application.builder().token(BOT_TOKEN).build()
            
            application.add_handler(CommandHandler("start", self.start))
            application.add_handler(CommandHandler("admin", self.admin_commands))
            application.add_handler(CallbackQueryHandler(self.search_handler))
            
            application.run_polling()
        except Exception as e:
            logger.error(f"Bot error: {e}")

if __name__ == "__main__":
    bot = ProductSearchBot()
    bot.run()