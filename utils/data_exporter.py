import pandas as pd
from datetime import datetime
import os
import logging
from typing import List, Dict

# Словарь соответствия городов и регионов России
CITY_TO_REGION = {
    'Москва': 'Москва', 'Санкт-Петербург': 'Санкт-Петербург',
    'Новосибирск': 'Новосибирская область', 'Екатеринбург': 'Свердловская область',
    'Казань': 'Республика Татарстан', 'Нижний Новгород': 'Нижегородская область',
    'Челябинск': 'Челябинская область', 'Самара': 'Самарская область',
    'Омск': 'Омская область', 'Ростов-на-Дону': 'Ростовская область',
    'Уфа': 'Республика Башкортостан', 'Красноярск': 'Красноярский край',
    'Пермь': 'Пермский край', 'Воронеж': 'Воронежская область',
    'Волгоград': 'Волгоградская область', 'Краснодар': 'Краснодарский край',
    'Саратов': 'Саратовская область', 'Тюмень': 'Тюменская область',
    'Тольятти': 'Самарская область', 'Ижевск': 'Удмуртская Республика',
    'Барнаул': 'Алтайский край', 'Ульяновск': 'Ульяновская область',
    'Иркутск': 'Иркутская область', 'Хабаровск': 'Хабаровский край',
    'Ярославль': 'Ярославская область', 'Владивосток': 'Приморский край',
    'Махачкала': 'Республика Дагестан', 'Томск': 'Томская область',
    'Оренбург': 'Оренбургская область', 'Кемерово': 'Кемеровская область',
    'Новокузнецк': 'Кемеровская область', 'Рязань': 'Рязанская область',
    'Астрахань': 'Астраханская область', 'Пенза': 'Пензенская область',
    'Липецк': 'Липецкая область', 'Тула': 'Тульская область',
    'Киров': 'Кировская область', 'Чебоксары': 'Чувашская Республика',
    'Калининград': 'Калининградская область', 'Брянск': 'Брянская область',
    'Курск': 'Курская область', 'Иваново': 'Ивановская область',
    'Магнитогорск': 'Челябинская область', 'Тверь': 'Тверская область',
    'Ставрополь': 'Ставропольский край', 'Белгород': 'Белгородская область',
    'Сочи': 'Краснодарский край', 'Нижний Тагил': 'Свердловская область',
    'Архангельск': 'Архангельская область', 'Владимир': 'Владимирская область',
    'Чита': 'Забайкальский край', 'Сургут': 'Ханты-Мансийский АО',
    'Калуга': 'Калужская область', 'Смоленск': 'Смоленская область',
    'Курган': 'Курганская область', 'Орёл': 'Орловская область',
    'Череповец': 'Вологодская область', 'Владикавказ': 'Республика Северная Осетия',
    'Мурманск': 'Мурманская область', 'Тамбов': 'Тамбовская область',
    'Грозный': 'Чеченская Республика', 'Стерлитамак': 'Республика Башкортостан',
    'Кострома': 'Костромская область', 'Петрозаводск': 'Республика Карелия',
    'Нижневартовск': 'Ханты-Мансийский АО', 'Новороссийск': 'Краснодарский край',
    'Йошкар-Ола': 'Республика Марий Эл', 'Таганрог': 'Ростовская область',
    'Волово': 'Липецкая область',
}


class DataExporter:
    @staticmethod
    def export_to_excel(vacancies: List[Dict], filename: str = None) -> str:
        """
        Экспорт только релевантных вакансий в Excel

        Args:
            vacancies: Список вакансий
            filename: Имя файла (опционально)

        Returns:
            Путь к созданному файлу
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"agro_vacancies_{timestamp}.xlsx"

        total_vacancies = len(vacancies)

        relevant_vacancies = [
            vac for vac in vacancies
            if vac.get('profession_code') != 'unknown'
               and vac.get('profession_code') is not None
        ]

        filtered_count = total_vacancies - len(relevant_vacancies)

        print(f"\nСтатистика фильтрации:")
        print(f"  - Всего собрано вакансий: {total_vacancies}")
        print(f"  - Релевантных вакансий: {len(relevant_vacancies)}")
        print(f"  - Отфильтровано нерелевантных: {filtered_count}")
        if total_vacancies > 0:
            print(f"  - Процент релевантных: {len(relevant_vacancies) / total_vacancies * 100:.1f}%")

        if not relevant_vacancies:
            print("Внимание: Нет релевантных вакансий для экспорта")
            # Создаем файл с пояснением
            empty_df = pd.DataFrame([{
                'Статус': 'Нет релевантных вакансий',
                'Всего найдено': total_vacancies,
                'Отфильтровано': filtered_count,
                'Причина': 'Все найденные вакансии не соответствуют сельскохозяйственным профессиям'
            }])
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                empty_df.to_excel(writer, sheet_name='Информация', index=False)
            print(f"Создан файл с предупреждением: {filename}")
            return filename

        # Создаем DataFrame с релевантными вакансиями
        df = pd.DataFrame(relevant_vacancies)

        # Переименовываем колонки для читаемости
        column_mapping = {
            'profession_code': 'Код профессии',
            'profession_name': 'Профессия',
            'title': 'Название вакансии',
            'city': 'Город',
            'salary_from': 'Зарплата от',
            'salary_to': 'Зарплата до',
            'salary_average': 'Зарплата средняя',
            'currency': 'Валюта',
            'source': 'Источник',
            'url': 'Ссылка',
            'company': 'Компания',
            'experience': 'Требуемый опыт',
            'employment': 'Тип занятости',
            'date_posted': 'Дата публикации'
        }

        # Применяем переименование только для существующих колонок
        existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_mapping)

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Основной лист с вакансиями
            df.to_excel(writer, sheet_name='Релевантные вакансии', index=False)

            # Лист со статистикой фильтрации
            stats_data = {
                'Показатель': [
                    'Всего собрано вакансий',
                    'Релевантных вакансий',
                    'Отфильтровано нерелевантных',
                    'Процент релевантных',
                    'Дата экспорта'
                ],
                'Значение': [
                    total_vacancies,
                    len(relevant_vacancies),
                    filtered_count,
                    f"{len(relevant_vacancies) / total_vacancies * 100:.1f}%" if total_vacancies > 0 else "0%",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
            }
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Статистика фильтрации', index=False)

            # Сводка по профессиям (только для релевантных вакансий)
            if 'Профессия' in df.columns and 'Зарплата средняя' in df.columns:
                summary_by_prof = df.groupby('Профессия').agg({
                    'Зарплата средняя': 'mean',
                    'Название вакансии': 'count'
                }).round(2)
                summary_by_prof.columns = ['Средняя зарплата (руб)', 'Количество вакансий']
                summary_by_prof = summary_by_prof.sort_values('Количество вакансий', ascending=False)
                summary_by_prof.to_excel(writer, sheet_name='Сводка по профессиям')

            # Сводка по городам
            if 'Город' in df.columns and 'Зарплата средняя' in df.columns:
                # Исключаем "Не указан" из сводки по городам
                city_df = df[df['Город'] != 'Не указан'].copy()
                if not city_df.empty:
                    summary_by_city = city_df.groupby('Город').agg({
                        'Зарплата средняя': 'mean',
                        'Название вакансии': 'count'
                    }).round(2)
                    summary_by_city.columns = ['Средняя зарплата (руб)', 'Количество вакансий']
                    summary_by_city = summary_by_city.sort_values('Количество вакансий', ascending=False)
                    summary_by_city.to_excel(writer, sheet_name='Сводка по городам')

            # Сводка по регионам (новая вкладка)
            if 'Город' in df.columns and 'Зарплата средняя' in df.columns:
                # Добавляем колонку с регионом
                df_with_regions = df.copy()
                df_with_regions['Регион'] = df_with_regions['Город'].apply(
                    lambda x: CITY_TO_REGION.get(x, 'Другие регионы') if x != 'Не указан' else 'Не указан'
                )
                
                # Группировка по регионам (исключая "Не указан")
                region_df = df_with_regions[df_with_regions['Регион'] != 'Не указан'].copy()
                if not region_df.empty:
                    summary_by_region = region_df.groupby('Регион').agg({
                        'Зарплата средняя': 'mean',
                        'Название вакансии': 'count'
                    }).round(2)
                    summary_by_region.columns = ['Средняя зарплата (руб)', 'Количество вакансий']
                    summary_by_region = summary_by_region.sort_values('Количество вакансий', ascending=False)
                    summary_by_region.to_excel(writer, sheet_name='Сводка по регионам')
                    
                    # Также добавляем сводку по всем трём сайтам
                    summary_by_region_source = region_df.groupby(['Регион', 'Источник']).size().unstack(fill_value=0)
                    summary_by_region_source = summary_by_region_source.sort_values('hh.ru', ascending=False)
                    summary_by_region_source.to_excel(writer, sheet_name='По регионам и источникам')

            # Сводка по источникам
            if 'Источник' in df.columns:
                summary_by_source = df.groupby('Источник').size().to_frame('Количество вакансий')
                summary_by_source = summary_by_source.sort_values('Количество вакансий', ascending=False)
                summary_by_source.to_excel(writer, sheet_name='Сводка по источникам')

            # Сводка по типу занятости
            if 'Тип занятости' in df.columns:
                employment_stats = df[df['Тип занятости'] != ''].groupby('Тип занятости').size()
                if not employment_stats.empty:
                    employment_df = employment_stats.to_frame('Количество вакансий')
                    employment_df.to_excel(writer, sheet_name='Сводка по занятости')

            # Автоматическое расширение столбцов
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

        print(f"\nДанные сохранены в файл: {filename}")
        print(f"Всего листов в файле: {len(writer.sheets) if 'writer' in locals() else 'N/A'}")

        return filename

    @staticmethod
    def export_to_csv(vacancies: List[Dict], filename: str = None) -> str:
        """
        Экспорт ТОЛЬКО релевантных вакансий в CSV

        Args:
            vacancies: Список вакансий
            filename: Имя файла (опционально)

        Returns:
            Путь к созданному файлу
        """
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"agro_vacancies_{timestamp}.csv"

        total_vacancies = len(vacancies)

        # Фильтруем только релевантные вакансии
        relevant_vacancies = [
            vac for vac in vacancies
            if vac.get('profession_code') != 'unknown'
               and vac.get('profession_code') is not None
        ]

        filtered_count = total_vacancies - len(relevant_vacancies)

        print(f"\nСтатистика фильтрации для CSV:")
        print(f"  - Всего собрано: {total_vacancies}")
        print(f"  - Экспортировано релевантных: {len(relevant_vacancies)}")
        print(f"  - Отфильтровано: {filtered_count}")

        if not relevant_vacancies:
            print("Нет релевантных вакансий для экспорта в CSV")
            # Создаем пустой CSV с пояснением
            import csv
            with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Статус', 'Всего найдено', 'Отфильтровано', 'Сообщение'])
                writer.writerow(['Нет релевантных вакансий', total_vacancies, filtered_count,
                                 'Все вакансии были отфильтрованы как нерелевантные'])
            print(f"Создан CSV файл с предупреждением: {filename}")
            return filename

        df = pd.DataFrame(relevant_vacancies)

        # Переименовываем колонки для читаемости
        column_mapping = {
            'profession_code': 'Код профессии',
            'profession_name': 'Профессия',
            'title': 'Название вакансии',
            'city': 'Город',
            'salary_from': 'Зарплата от',
            'salary_to': 'Зарплата до',
            'salary_average': 'Зарплата средняя',
            'currency': 'Валюта',
            'source': 'Источник',
            'url': 'Ссылка',
            'company': 'Компания',
            'experience': 'Требуемый опыт',
            'employment': 'Тип занятости',
            'date_posted': 'Дата публикации'
        }

        existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_mapping)

        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Данные сохранены в файл: {filename}")

        return filename

    @staticmethod
    def print_filtering_stats(vacancies: List[Dict]) -> None:
        """
        Вывод подробной статистики фильтрации вакансий

        Args:
            vacancies: Список всех вакансий
        """
        total = len(vacancies)
        relevant = [v for v in vacancies if v.get('profession_code') != 'unknown']
        irrelevant = [v for v in vacancies if v.get('profession_code') == 'unknown']

        print("СТАТИСТИКА ФИЛЬТРАЦИИ ВАКАНСИЙ")
        print(f"Релевантные вакансии: {len(relevant)} ({len(relevant) / total * 100:.1f}%)")
        print(f"Нерелевантные вакансии: {len(irrelevant)} ({len(irrelevant) / total * 100:.1f}%)")
        print(f"Всего вакансий: {total}")

        if irrelevant:
            print("\nПримеры нерелевантных вакансий (первые 10):")
            for i, vac in enumerate(irrelevant[:10], 1):
                title = vac.get('title', 'Нет названия')
                source = vac.get('source', 'Неизвестно')
                print(f"  {i}. {title[:80]}... (источник: {source})")

        if relevant:
            print("\nПримеры релевантных вакансий (первые 10):")
            for i, vac in enumerate(relevant[:10], 1):
                title = vac.get('title', 'Нет названия')
                code = vac.get('profession_code', 'unknown')
                print(f"  {i}. {title[:80]}... (код: {code})")
