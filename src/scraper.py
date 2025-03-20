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
        self.chunk_size = 50000
        self.df_cache = None
        self.search_index = {}  # Добавляем индекс для быстрого поиска
        
        os.makedirs(os.path.dirname(self.GISP_FILE_PATH), exist_ok=True)
        self.start_background_updates()
        
        if not os.path.exists(self.GISP_FILE_PATH):
            logger.info("GISP file not found, downloading...")
            self.download_gisp_file()
        logger.info("ProductScraper initialized successfully")

    async def download_gisp_file_with_status(self, status_message):
        temp_file = "data/temp_gisp.xlsx"
        try:
            # Этап 1: Скачивание файла
            logger.info("Starting GISP file download...")
            await status_message.edit_text("⏳ Скачивание файла ГИСП...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://gisp.gov.ru/',
            }

            self.GISP_EXCEL_URL = "https://gisp.gov.ru/pp719v2/mptapp/view/dl/production_res_valid_only/"
            
            try:
                response = requests.get(self.GISP_EXCEL_URL, headers=headers, verify=True, timeout=60)
                response.raise_for_status()
                
                # Сохраняем Excel файл
                with open(temp_file, 'wb') as f:
                    f.write(response.content)
                
                if not os.path.exists(temp_file) or os.path.getsize(temp_file) == 0:
                    raise Exception("Failed to download file or file is empty")
                
                logger.info(f"Excel file downloaded successfully, size: {os.path.getsize(temp_file)} bytes")
                
            except Exception as e:
                logger.error(f"Download failed: {str(e)}")
                raise Exception(f"Failed to download file: {str(e)}")

            # Этап 2: Обработка файла
            # Оптимизированная обработка Excel
            await status_message.edit_text("⏳ Обработка файла Excel...")
            try:
                logger.info(f"Starting Excel processing, file size: {os.path.getsize(temp_file)} bytes")
                
                # Оптимизированные настройки pandas
                pd.options.mode.chained_assignment = None
                chunk_size = 5000  # Уменьшаем размер чанка еще больше
                
                # Читаем только необходимые колонки с оптимизированными типами данных
                df_iterator = pd.read_excel(
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
                        'ТН ВЭД': str,
                        'Предприятие': str,
                        'Наименование продукции': str,
                        'Изготовлена по': str
                    },
                    chunksize=chunk_size
                )

                # Оптимизированная обработка чанков
                first_chunk = True
                total_rows = 0
                
                with open(self.GISP_FILE_PATH, 'w', encoding='utf-8-sig', newline='') as f:
                    for i, chunk in enumerate(df_iterator):
                        # Очищаем память перед обработкой нового чанка
                        import gc
                        gc.collect()
                        
                        # Обрабатываем чанк
                        chunk = chunk.dropna(how='all')
                        
                        # Создаем индексы только для текущего чанка
                        if first_chunk:
                            chunk.to_csv(f, index=False)
                            first_chunk = False
                        else:
                            chunk.to_csv(f, index=False, header=False)
                        
                        total_rows += len(chunk)
                        await status_message.edit_text(f"⏳ Обработано строк: {total_rows:,}")
                        
                        # Очищаем память
                        del chunk
                        gc.collect()

                logger.info(f"CSV file saved successfully, total rows: {total_rows}")
                
                # Обновляем индексы частями
                await status_message.edit_text("⏳ Создание индексов поиска...")
                self._update_search_index_by_chunks()
                
                # Удаляем временный файл
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                
                await status_message.edit_text("✅ Файл ГИСП успешно обновлен!")
                self.last_update = datetime.now()
                
                # Загружаем небольшую часть данных в кэш
                self.df_cache = pd.read_csv(
                    self.GISP_FILE_PATH, 
                    encoding='utf-8-sig',
                    dtype={
                        'ИНН': str,
                        'Реестровый номер': str,
                        'ОКПД2': str,
                        'ТН ВЭД': str
                    },
                    nrows=1000
                )
                
                return total_rows
                
            except Exception as e:
                logger.error(f"Excel processing failed: {str(e)}", exc_info=True)
                await status_message.edit_text(f"❌ Ошибка при обработке файла: {str(e)}")
                raise
                
        except Exception as e:
            logger.error(f"GISP file download failed: {str(e)}", exc_info=True)
            await status_message.edit_text(f"❌ Ошибка при загрузке файла: {str(e)}")
            return 0

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

    async def search_all(self, okpd2: Optional[str] = None, name: Optional[str] = None, status_message=None) -> List[Dict]:
        try:
            logger.info(f"Starting combined search with okpd2={okpd2}, name={name}")
            
            if status_message:
                await status_message.edit_text(
                    "🔍 Поиск в ЕАЭС...\n"
                    "⏳ Прогресс: 0%"
                )
            eaeu_results = self.search_eaeu(okpd2, name)
            
            if status_message:
                await status_message.edit_text(
                    "🔍 Поиск в ГИСП...\n"
                    "⏳ Прогресс: 50%"
                )
            gisp_results = await self.search_gisp(okpd2, name, status_message)
            
            total_results = eaeu_results + gisp_results
            
            if status_message:
                await status_message.edit_text(
                    f"✅ Поиск завершен\n"
                    f"📊 Всего найдено: {len(total_results)}\n"
                    f"ЕАЭС: {len(eaeu_results)}\n"
                    f"ГИСП: {len(gisp_results)}\n\n"
                    f"Используйте /start для нового поиска"
                )
            
            logger.info(f"Combined search completed, total results: {len(total_results)}")
            return total_results
            
        except Exception as e:
            logger.error(f"Combined search error: {e}")
            if status_message:
                await status_message.edit_text(
                    f"❌ Ошибка при поиске: {str(e)}\n\n"
                    f"Используйте /start для нового поиска"
                )
            return []

    def _update_search_index_by_chunks(self):
        """Создает индексы для быстрого поиска, обрабатывая файл частями"""
        logger.info("Updating search indexes by chunks...")
        self.search_index = {
            'okpd2': {},
            'name': set()
        }
        
        # Используем небольшой размер чанка для экономии памяти
        chunk_size = 5000
        total_processed = 0
        
        try:
            for chunk in pd.read_csv(
                self.GISP_FILE_PATH, 
                encoding='utf-8-sig',
                dtype={
                    'ИНН': str,
                    'Реестровый номер': str,
                    'ОКПД2': str,
                    'ТН ВЭД': str
                },
                chunksize=chunk_size
            ):
                # Индекс для ОКПД2
                for idx, code in enumerate(chunk['ОКПД2']):
                    if pd.notna(code):
                        code = str(code).lower()
                        # Ограничиваем длину префиксов для экономии памяти
                        max_prefix_len = min(len(code), 5)  # Еще меньше префиксов
                        for i in range(max_prefix_len):
                            prefix = code[:i+1]
                            if prefix not in self.search_index['okpd2']:
                                self.search_index['okpd2'][prefix] = set()
                            # Используем глобальный индекс
                            self.search_index['okpd2'][prefix].add(total_processed + idx)
                
                # Индекс для наименований (только уникальные значения)
                self.search_index['name'].update(
                    set(chunk['Наименование продукции'].str.lower().dropna())
                )
                
                total_processed += len(chunk)
                
                # Принудительная очистка памяти
                import gc
                del chunk
                gc.collect()
            
            logger.info(f"Search indexes updated successfully, processed {total_processed} rows")
            
        except Exception as e:
            logger.error(f"Error updating search indexes: {e}", exc_info=True)
            # Создаем пустые индексы в случае ошибки
            self.search_index = {'okpd2': {}, 'name': set()}
        
        # Удаляем этот код, так как переменная df не определена
        # Индекс для ОКПД2
        # for idx, code in enumerate(df['ОКПД2']):
        #     if pd.notna(code):
        #         code = str(code).lower()
        #         for i in range(len(code)):
        #             prefix = code[:i+1]
        #             if prefix not in self.search_index['okpd2']:
        #                 self.search_index['okpd2'][prefix] = set()
        #             self.search_index['okpd2'][prefix].add(idx)
        
        # # Индекс для наименований
        # self.search_index['name'] = set(df['Наименование продукции'].str.lower().dropna())
        # logger.info("Search indexes updated successfully")

    async def search_gisp(self, okpd2: Optional[str] = None, name: Optional[str] = None, status_message=None) -> List[Dict]:
        try:
            if status_message:
                await status_message.edit_text("🔍 Начинаем поиск в ГИСП...")

            # Используем кэш, если он есть
            if self.df_cache is None:
                if status_message:
                    await status_message.edit_text("📖 Загрузка базы данных...")
                
                self.df_cache = pd.read_csv(
                    self.GISP_FILE_PATH,
                    encoding='utf-8-sig',
                    dtype={
                        'ИНН': str,
                        'Реестровый номер': str,
                        'ОКПД2': str,
                        'ТН ВЭД': str
                    }
                )
                # Заменяем вызов несуществующего метода
                self._update_search_index_by_chunks()

            df = self.df_cache
            total_rows = len(df)

            if status_message:
                await status_message.edit_text("🔍 Применение фильтров...")

            # Используем индексы для быстрого поиска
            if okpd2 and name:
                okpd2_lower = okpd2.lower()
                name_lower = name.lower()
                
                # Поиск по ОКПД2
                potential_indices = self.search_index['okpd2'].get(okpd2_lower, set())
                mask = df.index.isin(potential_indices)
                
                # Дополнительная фильтрация по наименованию
                name_mask = df['Наименование продукции'].str.lower().str.contains(name_lower, na=False)
                mask = mask & name_mask
                
            elif okpd2:
                okpd2_lower = okpd2.lower()
                potential_indices = self.search_index['okpd2'].get(okpd2_lower, set())
                mask = df.index.isin(potential_indices)
                
            elif name:
                name_lower = name.lower()
                mask = df['Наименование продукции'].str.lower().str.contains(name_lower, na=False)
            else:
                return []

            # Получаем результаты
            results = df[mask].to_dict('records')

            if status_message:
                await status_message.edit_text("📊 Форматирование результатов...")
            
            # Преобразуем результаты в нужный формат
            formatted_results = [{
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
            } for row in results]

            if status_message:
                found_count = len(formatted_results)
                await status_message.edit_text(
                    f"✅ Поиск завершен\n"
                    f"📊 Найдено результатов: {found_count}\n"
                    f"💾 Всего записей в базе: {total_rows}"
                )

            logger.info(f"GISP search completed, found {len(formatted_results)} results")
            return formatted_results

        except Exception as e:
            logger.error(f"GISP search error: {e}")
            if status_message:
                await status_message.edit_text(f"❌ Ошибка при поиске: {str(e)}")
            return []

    def start_background_updates(self):
        """Запускает фоновое обновление файла ГИСП"""
        try:
            def update_job():
                logger.info("Running scheduled GISP update...")
                self.download_gisp_file()
                
            # Обновляем файл каждые 7 дней
            schedule.every(7).days.do(update_job)
            
            # Запускаем планировщик в отдельном потоке
            def run_scheduler():
                while True:
                    try:
                        schedule.run_pending()
                        time.sleep(3600)  # Проверяем раз в час
                    except Exception as e:
                        logger.error(f"Scheduler error: {e}")
                        time.sleep(3600)  # В случае ошибки ждем час и пробуем снова
            
            scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
            scheduler_thread.start()
            logger.info("Background updates scheduled")
            
        except Exception as e:
            logger.error(f"Failed to start background updates: {e}")

    def download_gisp_file(self):
        """Скачивает файл ГИСП без отображения статуса"""
        try:
            logger.info("Starting GISP file download (no status)...")
            temp_file = "data/temp_gisp.xlsx"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://gisp.gov.ru/',
            }
            
            self.GISP_EXCEL_URL = "https://gisp.gov.ru/pp719v2/mptapp/view/dl/production_res_valid_only/"
            
            response = requests.get(self.GISP_EXCEL_URL, headers=headers, verify=True, timeout=60)
            response.raise_for_status()
            
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            logger.info("Processing GISP file...")
            
            # Оптимизированная обработка без статуса
            chunk_size = 5000
            first_chunk = True
            total_rows = 0
            
            with open(self.GISP_FILE_PATH, 'w', encoding='utf-8-sig', newline='') as f:
                for chunk in pd.read_excel(
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
                    },
                    chunksize=chunk_size
                ):
                    # Очистка памяти
                    import gc
                    gc.collect()
                    
                    # Обработка чанка
                    chunk = chunk.dropna(how='all')
                    
                    # Запись в CSV
                    if first_chunk:
                        chunk.to_csv(f, index=False)
                        first_chunk = False
                    else:
                        chunk.to_csv(f, index=False, header=False)
                    
                    total_rows += len(chunk)
                    
                    # Очистка памяти
                    del chunk
                    gc.collect()
            
            # Удаляем временный файл
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            # Обновляем индексы
            self._update_search_index_by_chunks()
            
            # Загружаем небольшую часть данных в кэш
            self.df_cache = pd.read_csv(
                self.GISP_FILE_PATH, 
                encoding='utf-8-sig',
                dtype={
                    'ИНН': str,
                    'Реестровый номер': str,
                    'ОКПД2': str,
                    'ТН ВЭД': str
                },
                nrows=1000
            )
            
            self.last_update = datetime.now()
            logger.info(f"GISP file updated successfully, total rows: {total_rows}")
            
        except Exception as e:
            logger.error(f"GISP file download failed: {e}", exc_info=True)
