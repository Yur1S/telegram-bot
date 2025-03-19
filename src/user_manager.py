import json
import os
from typing import List, Dict

class UserManager:
    def __init__(self, users_file: str = 'allowed_users.json'):
        self.users_file = users_file
        self.allowed_users = self._load_users()
        
    def _load_users(self) -> Dict:
        if os.path.exists(self.users_file):
            with open(self.users_file, 'r') as f:
                return json.load(f)
        return {"usernames": [], "phone_numbers": [], "admins": []}
        
    def _save_users(self):
        with open(self.users_file, 'w') as f:
            json.dump(self.allowed_users, f, indent=4)
            
    def is_allowed(self, username: str = None, phone: str = None) -> bool:
        if username in self.allowed_users["usernames"]:
            return True
        if phone in self.allowed_users["phone_numbers"]:
            return True
        return False
        
    def is_admin(self, username: str) -> bool:
        return username in self.allowed_users["admins"]
        
    def add_user(self, username: str = None, phone: str = None):
        if username and username not in self.allowed_users["usernames"]:
            self.allowed_users["usernames"].append(username)
        if phone and phone not in self.allowed_users["phone_numbers"]:
            self.allowed_users["phone_numbers"].append(phone)
        self._save_users()
        
    def remove_user(self, username: str = None, phone: str = None):
        if username and username in self.allowed_users["usernames"]:
            self.allowed_users["usernames"].remove(username)
        if phone and phone in self.allowed_users["phone_numbers"]:
            self.allowed_users["phone_numbers"].remove(phone)
        self._save_users()