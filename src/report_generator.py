import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

class ReportGenerator:
    @staticmethod
    def generate_excel_report(data):
        try:
            df = pd.DataFrame(data)
            excel_file = BytesIO()
            df.to_excel(excel_file, index=False, engine='openpyxl')
            excel_file.seek(0)
            return excel_file
        except Exception as e:
            logger.error(f"Excel report generation error: {e}")
            return None

    @staticmethod
    def generate_text_report(data):
        try:
            df = pd.DataFrame(data)
            text_file = BytesIO()
            df.to_string(text_file, index=False)
            text_file.seek(0)
            return text_file
        except Exception as e:
            logger.error(f"Text report generation error: {e}")
            return None