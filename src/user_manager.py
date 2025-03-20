import json
import logging
import os

logger = logging.getLogger(__name__)

class UserManager:
    def __init__(self):
        self.users_file = 'data/users.json'
        self.allowed_users = self._load_users()
        logger.debug(f"UserManager initialized with users: {self.allowed_users}")

    def _load_users(self) -> dict:
        try:
            if not os.path.exists(self.users_file):
                logger.debug("Users file not found, creating default structure")
                default_users = {"admins": [], "usernames": []}
                self._save_users(default_users)
                return default_users
            
            with open(self.users_file, 'r', encoding='utf-8') as f:
                users = json.load(f)
                logger.debug(f"Loaded users from file: {users}")
                return users
        except Exception as e:
            logger.error(f"Error loading users: {e}", exc_info=True)
            return {"admins": [], "usernames": []}

    def _save_users(self, users=None):
        try:
            if users is None:
                users = self.allowed_users
            
            os.makedirs(os.path.dirname(self.users_file), exist_ok=True)
            with open(self.users_file, 'w', encoding='utf-8') as f:
                json.dump(users, f, ensure_ascii=False, indent=2)
            logger.debug(f"Users saved successfully: {users}")
        except Exception as e:
            logger.error(f"Error saving users: {e}", exc_info=True)

    def is_admin(self, username: str) -> bool:
        if not username:
            return False
        return username in self.allowed_users.get("admins", [])

    def is_allowed(self, username: str) -> bool:
        if not username:
            return False
        return (username in self.allowed_users.get("usernames", []) or 
                username in self.allowed_users.get("admins", []))

    def add_user(self, username: str):
        try:
            if not username:
                logger.warning("Attempted to add empty username")
                return
            
            if username not in self.allowed_users["usernames"]:
                self.allowed_users["usernames"].append(username)
                self._save_users()
                logger.info(f"User {username} added successfully")
            else:
                logger.debug(f"User {username} already exists")
        except Exception as e:
            logger.error(f"Error adding user {username}: {e}", exc_info=True)

    def remove_user(self, username: str):
        try:
            if not username:
                logger.warning("Attempted to remove empty username")
                return
            
            if username in self.allowed_users["usernames"]:
                self.allowed_users["usernames"].remove(username)
                self._save_users()
                logger.info(f"User {username} removed successfully")
            else:
                logger.debug(f"User {username} not found in allowed users")
        except Exception as e:
            logger.error(f"Error removing user {username}: {e}", exc_info=True)

    def get_all_users(self) -> list:
        try:
            all_users = (
                self.allowed_users.get("usernames", []) + 
                self.allowed_users.get("admins", [])
            )
            return list(set(all_users))  # Remove duplicates
        except Exception as e:
            logger.error(f"Error getting all users: {e}", exc_info=True)
            return []

    def get_admins(self) -> list:
        try:
            return self.allowed_users.get("admins", [])
        except Exception as e:
            logger.error(f"Error getting admins: {e}", exc_info=True)
            return []