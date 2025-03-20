import requests
import json
from typing import List, Dict, Optional
import logging
import pandas as pd
import os
from datetime import datetime, timedelta
import schedule
import time
import threading
import asyncio

logger = logging.getLogger(__name__)

class ProductScraper:
    def __init__(self):
        logger.info("Initializing ProductScraper...")
        self.EAEU_API_URL = "https://goszakupki.eaeunion.org/spd/find"
        self.GISP_EXCEL_URL = "https://gisp.gov.ru/documents/10546/11962150/reestr_pprf_719_27122023.xlsx"
        self.GISP_FILE_PATH = "data/gisp_products.csv"
        self.last_update = None
        self.file_update_status = None
        
        os.makedirs(os.path.dirname(self.GISP_FILE_PATH), exist_ok=True)
        self.start_background_updates()
        
        if not os.path.exists(self.GISP_FILE_PATH):
            logger.info("GISP file not found, downloading...")
            self.download_gisp_file()
        logger.info("ProductScraper initialized successfully")

    async def download_gisp_file_with_status(self, status_message):
        temp_file = "data/temp_gisp.xlsx"
        try:
            logger.info("Starting GISP file download with status updates...")
            await status_message.edit_text("⏳ Скачивание файла ГИСП...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://gisp.gov.ru/',
            }
            
            logger.debug(f"Sending request to {self.GISP_EXCEL_URL}")
            try:
                response = requests.get(self.GISP_EXCEL_URL, headers=headers, verify=True, timeout=60, stream=True)
                logger.debug(f"Response status: {response.status_code}")
                logger.debug(f"Response headers: {dict(response.headers)}")
                
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                raise Exception(f"Failed to download file: {str(e)}")

            # Проверяем размер файла
            content_length = int(response.headers.get('content-length', 0))
            logger.debug(f"Content length: {content_length} bytes")
            if content_length == 0:
                raise Exception("Empty file received")

            # Скачиваем файл чанками
            with open(temp_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(temp_file)
            logger.debug(f"Downloaded file size: {file_size} bytes")
            if file_size == 0:
                raise Exception("Downloaded file is empty")

            await status_message.edit_text("⏳ Обработка файла...")
            df = pd.read_excel(
                temp_file,
                usecols=[0, 1, 6, 8, 9, 11, 12, 13, 14],
                skiprows=2,
                names=[
                    'Предприятие', 'ИНН', 'Реестровый номер', 
                    'Дата внесения в реестр', 'Срок действия',
                    'Наименование продукции', 'ОКПД2', 'ТН ВЭД', 'Изготовлена по'
                ],
                engine='openpyxl',
                dtype={
                    'ИНН': str,
                    'Реестровый номер': str,
                    'ОКПД2': str,
                    'ТН ВЭД': str
                }
            )
            
            await status_message.edit_text("⏳ Оптимизация данных...")
            df = df.dropna(how='all')
            df = df.reset_index(drop=True)
            
            await status_message.edit_text("⏳ Сохранение файла...")
            df.to_csv(self.GISP_FILE_PATH, index=False)
            
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            self.last_update = datetime.now()
            logger.info(f"GISP file updated successfully with status at {self.last_update}")
            
        except Exception as e:
            logger.error(f"Error downloading/optimizing GISP file with status: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise e

    def download_gisp_file(self):
        try:
            logger.info("Starting GISP file download...")
            if self.file_update_status:
                asyncio.run(self.download_gisp_file_with_status(self.file_update_status))
            else:
                response = requests.get(self.GISP_EXCEL_URL)
                response.raise_for_status()
                
                temp_file = "data/temp_gisp.xlsx"
                with open(temp_file, 'wb') as f:
                    f.write(response.content)
                
                logger.info("Processing GISP file...")
                df = pd.read_excel(
                    temp_file,
                    usecols=[0, 1, 6, 8, 9, 11, 12, 13, 14],
                    skiprows=2,
                    names=[
                        'Предприятие', 'ИНН', 'Реестровый номер', 
                        'Дата внесения в реестр', 'Срок действия',
                        'Наименование продукции', 'ОКПД2', 'ТН ВЭД', 'Изготовлена по'
                    ],
                    engine='openpyxl',
                    dtype={
                        'ИНН': str,
                        'Реестровый номер': str,
                        'ОКПД2': str,
                        'ТН ВЭД': str
                    }
                )
                
                logger.info("Optimizing GISP data...")
                df = df.dropna(how='all')
                df = df.reset_index(drop=True)
                
                logger.info("Saving GISP file...")
                df.to_csv(self.GISP_FILE_PATH, index=False)
                
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                
                self.last_update = datetime.now()
                logger.info(f"GISP file updated successfully at {self.last_update}")
                
        except Exception as e:
            logger.error(f"Error downloading/optimizing GISP file: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def update_scheduler(self):
        while True:
            schedule.run_pending()
            time.sleep(3600)

    def start_background_updates(self):
        logger.info("Starting background updates scheduler...")
        schedule.every().day.at("00:00").do(self.download_gisp_file)
        thread = threading.Thread(target=self.update_scheduler, daemon=True)
        thread.start()
        logger.info("Background updates scheduler started")

    def search_gisp(self, okpd2: Optional[str] = None, name: Optional[str] = None) -> List[Dict]:
        try:
            logger.info(f"Starting GISP search with okpd2={okpd2}, name={name}")
            if not os.path.exists(self.GISP_FILE_PATH):
                logger.warning("GISP file not found, downloading...")
                self.download_gisp_file()
                if not os.path.exists(self.GISP_FILE_PATH):
                    logger.error("Failed to download GISP file")
                    return []

            df = pd.read_csv(self.GISP_FILE_PATH)
            
            if name:
                name = name.lower()
            if okpd2:
                okpd2 = okpd2.lower()
            
            results = []
            
            if okpd2 and name:
                mask = (
                    df['ОКПД2'].str.lower().str.contains(okpd2, na=False) &
                    df['Наименование продукции'].str.lower().str.contains(name, na=False)
                )
            elif okpd2:
                mask = df['ОКПД2'].str.lower().str.contains(okpd2, na=False)
            elif name:
                mask = df['Наименование продукции'].str.lower().str.contains(name, na=False)
            else:
                return []

            for _, row in df[mask].iterrows():
                results.append({
                    'name': row['Наименование продукции'],
                    'okpd2_code': row['ОКПД2'],
                    'manufacturer': row['Предприятие'],
                    'inn': row['ИНН'],
                    'registry_number': row['Реестровый номер'],
                    'registry_date': row['Дата внесения в реестр'],
                    'valid_until': row['Срок действия'],
                    'tn_ved': row['ТН ВЭД'],
                    'standard': row['Изготовлена по'],
                    'source': 'ГИСП'
                })

            logger.info(f"GISP search completed, found {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"GISP search error: {e}")
            return []

    def search_eaeu(self, okpd2: Optional[str] = None, name: Optional[str] = None) -> List[Dict]:
        try:
            logger.info(f"Starting EAEU search with okpd2={okpd2}, name={name}")
            params = {
                "collection": "db1.v_goodscollection_prod_public",
                "limit": 1000,
                "skip": 0,
                "sort": {"publishdate": -1}
            }

            query_filter = {}
            if okpd2:
                query_filter["okpd2.code"] = {"$regex": f"^{okpd2}", "$options": "i"}
            if name:
                query_filter["name"] = {"$regex": name, "$options": "i"}

            if query_filter:
                params["filter"] = query_filter

            response = requests.post(self.EAEU_API_URL, json=params)
            response.raise_for_status()
            
            data = response.json()
            
            results = []
            for item in data.get('items', []):
                result = {
                    'name': item.get('name', ''),
                    'okpd2_code': item.get('okpd2', {}).get('code', ''),
                    'manufacturer': item.get('manufacturer', {}).get('name', ''),
                    'inn': '',
                    'registry_number': '',
                    'registry_date': '',
                    'valid_until': '',
                    'tn_ved': '',
                    'standard': '',
                    'source': 'ЕАЭС'
                }
                results.append(result)
            
            logger.info(f"EAEU search completed, found {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"EAEU API search error: {e}")
            return []

    def search_all(self, okpd2: Optional[str] = None, name: Optional[str] = None) -> List[Dict]:
        logger.info(f"Starting combined search with okpd2={okpd2}, name={name}")
        eaeu_results = self.search_eaeu(okpd2, name)
        gisp_results = self.search_gisp(okpd2, name)
        total_results = eaeu_results + gisp_results
        logger.info(f"Combined search completed, total results: {len(total_results)}")
        return total_results