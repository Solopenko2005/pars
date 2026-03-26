import pandas as pd
from datetime import datetime
import os


class DataExporter:
    @staticmethod
    def export_to_excel(vacancies: list, filename: str = None):
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"agro_vacancies_{timestamp}.xlsx"

        df = pd.DataFrame(vacancies)

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Все вакансии', index=False)

            if 'profession_name' in df.columns and 'salary_average' in df.columns:
                summary_by_prof = df.groupby('profession_name').agg({
                    'salary_average': 'mean',
                    'title': 'count'
                }).round(2)
                summary_by_prof.columns = ['Средняя зарплата', 'Количество вакансий']
                summary_by_prof.to_excel(writer, sheet_name='Сводка по профессиям')

            if 'city' in df.columns and 'salary_average' in df.columns:
                summary_by_city = df.groupby('city').agg({
                    'salary_average': 'mean',
                    'title': 'count'
                }).round(2)
                summary_by_city.columns = ['Средняя зарплата', 'Количество вакансий']
                summary_by_city.to_excel(writer, sheet_name='Сводка по городам')

        print(f"Данные сохранены в файл: {filename}")
        return filename

    @staticmethod
    def export_to_csv(vacancies: list, filename: str = None):
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"agro_vacancies_{timestamp}.csv"

        df = pd.DataFrame(vacancies)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Данные сохранены в файл: {filename}")
        return filename