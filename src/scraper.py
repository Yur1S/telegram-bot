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

logger = logging.getLogger(__name__)

class ProductScraper:
    def __init__(self):
        self.EAEU_API_URL = "https://goszakupki.eaeunion.org/spd/find"
        self.GISP_EXCEL_URL = "https://gisp.gov.ru/pp719v2/mptapp/view/dl/production_res_valid_only/"
        self.GISP_FILE_PATH = "data/gisp_products.xlsx"
        self.last_update = None
        
        os.makedirs(os.path.dirname(self.GISP_FILE_PATH), exist_ok=True)
        self.start_background_updates()

    def download_gisp_file(self):
        try:
            response = requests.get(self.GISP_EXCEL_URL)
            response.raise_for_status()
            
            if os.path.exists(self.GISP_FILE_PATH):
                os.remove(self.GISP_FILE_PATH)
            
            with open(self.GISP_FILE_PATH, 'wb') as f:
                f.write(response.content)
            
            self.last_update = datetime.now()
            logger.info(f"GISP Excel file updated successfully at {self.last_update}")
            
        except Exception as e:
            logger.error(f"Error downloading GISP file: {e}")

    def update_scheduler(self):
        while True:
            schedule.run_pending()
            time.sleep(3600)

    def start_background_updates(self):
        schedule.every().day.at("00:00").do(self.download_gisp_file)
        thread = threading.Thread(target=self.update_scheduler, daemon=True)
        thread.start()
        self.download_gisp_file()

    def search_gisp(self, okpd2: Optional[str] = None, name: Optional[str] = None) -> List[Dict]:
        try:
            if not os.path.exists(self.GISP_FILE_PATH):
                logger.error("GISP Excel file not found")
                return []

            df = pd.read_excel(
                self.GISP_FILE_PATH,
                usecols=[11, 12],
                skiprows=2,
                names=['Наименование продукции', 'ОКПД2']
            )
            
            df = df.dropna(how='all')
            
            if name:
                name = name.lower()
            if okpd2:
                okpd2 = okpd2.lower()
            
            chunk_size = 1000
            results = []
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                
                if okpd2 and name:
                    mask = (
                        chunk['ОКПД2'].str.lower().str.contains(okpd2, na=False) &
                        chunk['Наименование продукции'].str.lower().str.contains(name, na=False)
                    )
                elif okpd2:
                    mask = chunk['ОКПД2'].str.lower().str.contains(okpd2, na=False)
                elif name:
                    mask = chunk['Наименование продукции'].str.lower().str.contains(name, na=False)
                else:
                    continue

                for _, row in chunk[mask].iterrows():
                    results.append({
                        'name': row['Наименование продукции'],
                        'okpd2_code': row['ОКПД2'],
                        'manufacturer': '',
                        'source': 'ГИСП'
                    })

                del chunk
                
                if len(results) >= 100:
                    break

            return results[:100]

        except Exception as e:
            logger.error(f"GISP Excel search error: {e}")
            return []

    def search_eaeu(self, okpd2: Optional[str] = None, name: Optional[str] = None) -> List[Dict]:
        try:
            params = {
                "collection": "db1.v_goodscollection_prod_public",
                "limit": 100,
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
                    'okpd2_name': item.get('okpd2', {}).get('name', ''),
                    'manufacturer': item.get('manufacturer', {}).get('name', ''),
                    'publish_date': item.get('publishdate', ''),
                    'source': 'ЕАЭС'
                }
                results.append(result)
            
            return results

        except Exception as e:
            logger.error(f"EAEU API search error: {e}")
            return []

    def search_all(self, okpd2: Optional[str] = None, name: Optional[str] = None) -> List[Dict]:
        eaeu_results = self.search_eaeu(okpd2, name)
        gisp_results = self.search_gisp(okpd2, name)
        return eaeu_results + gisp_results