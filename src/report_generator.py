import pandas as pd
from io import BytesIO
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class ReportGenerator:
    def generate_excel_report(self, results: List[Dict]) -> BytesIO:
        try:
            df = pd.DataFrame(results)
            
            columns_map = {
                'name': 'Наименование продукции',
                'okpd2_code': 'ОКПД2',
                'manufacturer': 'Предприятие',
                'inn': 'ИНН',
                'registry_number': 'Реестровый номер',
                'registry_date': 'Дата внесения в реестр',
                'valid_until': 'Срок действия',
                'tn_ved': 'ТН ВЭД',
                'standard': 'Изготовлена по',
                'source': 'Источник'
            }
            
            df = df.rename(columns=columns_map)
            
            column_order = [
                'Предприятие', 'ИНН', 'Реестровый номер', 
                'Дата внесения в реестр', 'Срок действия',
                'Наименование продукции', 'ОКПД2', 'ТН ВЭД', 
                'Изготовлена по', 'Источник'
            ]
            df = df[column_order]
            
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='Результаты поиска')
                
                worksheet = writer.sheets['Результаты поиска']
                for idx, col in enumerate(df.columns):
                    # Автоматическая настройка ширины столбцов
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    ) + 2
                    worksheet.set_column(idx, idx, max_length)
                
                # Форматирование заголовков
                header_format = writer.book.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'align': 'center',
                    'border': 1
                })
                
                # Применяем форматирование к заголовкам
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Форматирование данных
                data_format = writer.book.add_format({
                    'text_wrap': True,
                    'valign': 'top',
                    'border': 1
                })
                
                # Применяем форматирование к данным
                for row in range(len(df)):
                    for col in range(len(df.columns)):
                        worksheet.write(row + 1, col, df.iloc[row, col], data_format)
            
            output.seek(0)
            return output
        
        except Exception as e:
            logger.error(f"Error generating Excel report: {e}")
            return None