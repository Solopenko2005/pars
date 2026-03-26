# main.py
import time
import logging
from datetime import datetime
from typing import List, Dict
from professions import SEARCH_TERMS, PROFESSIONS
from parsers.hh_parser import HHParser
from parsers.superjob_parser import SuperJobParser
from parsers.yavagro_parser import YavagroParser
from parsers.svoevagro_parser import SvoevagroParser
from utils.data_exporter import DataExporter
from config import DELAY, PARSE_ALL_RUSSIA

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'parser_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)


class AgroVacancyParser:
    def __init__(self):
        self.hh_parser = HHParser()
        self.superjob_parser = SuperJobParser()
        self.yavagro_parser = YavagroParser()
        self.svoevagro_parser = SvoevagroParser()
        self.exporter = DataExporter()
        self.all_vacancies = []

        logging.info(
            f"Инициализация парсера. Режим: {'все регионы России' if PARSE_ALL_RUSSIA else 'выборочные регионы'}")

    def parse_all_professions(self):
        total_professions = len(SEARCH_TERMS)

        for idx, profession in enumerate(SEARCH_TERMS, 1):
            logging.info(f"[{idx}/{total_professions}] Парсинг профессии: {profession}")

            start_time = time.time()

            sources = [
                ("HH.ru", self.hh_parser.search_vacancies),
                ("SuperJob", self.superjob_parser.search_vacancies),
                ("Yavagro", self.yavagro_parser.search_vacancies),
                ("Svoevagro", self.svoevagro_parser.search_vacancies)
            ]

            for source_name, parse_func in sources:
                self._parse_profession_from_source(profession, source_name, parse_func)
                time.sleep(DELAY)

            elapsed_time = time.time() - start_time
            prof_count = len([v for v in self.all_vacancies if v['profession_name'] == profession])
            logging.info(f"  Профессия '{profession}': найдено {prof_count} вакансий, время: {elapsed_time:.2f} сек")

    def _parse_profession_from_source(self, profession: str, source_name: str, parse_func):
        try:
            logging.info(f"  Парсинг {source_name}...")
            vacancies = parse_func(profession)

            if vacancies:
                self.all_vacancies.extend(vacancies)
                logging.info(f"    Найдено вакансий: {len(vacancies)}")

                # Выводим пример первой вакансии для проверки
                if vacancies and len(vacancies) > 0:
                    first = vacancies[0]
                    logging.debug(
                        f"Пример: {first['title']} - {first['city']} - {first.get('salary_average', 'не указана')} руб")
            else:
                logging.info(f"Вакансий не найдено")

        except Exception as e:
            logging.error(f"Ошибка при парсинге {source_name}: {e}")

    def get_statistics(self):
        if not self.all_vacancies:
            logging.warning("Нет данных для статистики")
            return

        logging.info("\n" + "=" * 50)
        logging.info("СТАТИСТИКА ПАРСИНГА")
        logging.info("=" * 50)

        total_vacancies = len(self.all_vacancies)
        logging.info(f"Всего вакансий собрано: {total_vacancies}")

        # Статистика по источникам
        sources = {}
        for vac in self.all_vacancies:
            source = vac['source']
            sources[source] = sources.get(source, 0) + 1

        logging.info("\nПо источникам:")
        for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
            logging.info(f"  {source}: {count} вакансий ({count / total_vacancies * 100:.1f}%)")

        # Статистика по профессиям
        professions_count = {}
        for vac in self.all_vacancies:
            prof = vac['profession_name']
            professions_count[prof] = professions_count.get(prof, 0) + 1

        logging.info("\nТоп-10 профессий по количеству вакансий:")
        sorted_profs = sorted(professions_count.items(), key=lambda x: x[1], reverse=True)[:10]
        for prof, count in sorted_profs:
            logging.info(f"  {prof}: {count} вакансий")

        # Статистика по городам
        cities_count = {}
        for vac in self.all_vacancies:
            city = vac['city']
            if city != 'Не указан':
                cities_count[city] = cities_count.get(city, 0) + 1

        logging.info("\nТоп-10 городов по количеству вакансий:")
        sorted_cities = sorted(cities_count.items(), key=lambda x: x[1], reverse=True)[:10]
        for city, count in sorted_cities:
            logging.info(f"  {city}: {count} вакансий")

        # Статистика по зарплатам
        salaries = [v['salary_average'] for v in self.all_vacancies if v.get('salary_average')]
        if salaries:
            avg_salary = sum(salaries) / len(salaries)
            max_salary = max(salaries)
            min_salary = min(salaries)
            logging.info(f"\nСтатистика по зарплатам:")
            logging.info(f"  Средняя зарплата: {avg_salary:,.0f} руб.")
            logging.info(f"  Максимальная: {max_salary:,.0f} руб.")
            logging.info(f"  Минимальная: {min_salary:,.0f} руб.")

    def filter_vacancies_by_city(self, city_name: str) -> List[Dict]:
        return [v for v in self.all_vacancies if city_name.lower() in v['city'].lower()]

    def filter_vacancies_by_salary(self, min_salary: int = None, max_salary: int = None) -> List[Dict]:
        filtered = self.all_vacancies
        if min_salary:
            filtered = [v for v in filtered if v.get('salary_average', 0) >= min_salary]
        if max_salary:
            filtered = [v for v in filtered if v.get('salary_average', float('inf')) <= max_salary]
        return filtered


def main():
    logging.info("=" * 50)
    logging.info("ПАРСЕР АГРАРНЫХ ВАКАНСИЙ")
    logging.info("=" * 50)
    logging.info(f"Всего профессий для парсинга: {len(SEARCH_TERMS)}")
    logging.info(f"Режим парсинга: {'все города России' if PARSE_ALL_RUSSIA else 'выборочные города'}")
    logging.info("Начинаем сбор данных...\n")

    parser = AgroVacancyParser()

    try:
        start_time = time.time()
        parser.parse_all_professions()
        elapsed_time = time.time() - start_time

        parser.get_statistics()

        logging.info(f"\nОбщее время выполнения: {elapsed_time / 60:.2f} минут")

        if parser.all_vacancies:
            logging.info("\nСохраняем результаты...")
            excel_file = parser.exporter.export_to_excel(parser.all_vacancies)
            csv_file = parser.exporter.export_to_csv(parser.all_vacancies)

            logging.info(f"\nРезультаты сохранены:")
            logging.info(f"  Excel: {excel_file}")
            logging.info(f"  CSV: {csv_file}")
            logging.info(f"  Всего записей: {len(parser.all_vacancies)}")
        else:
            logging.warning("\nВакансии не найдены. Проверьте настройки парсинга.")

    except KeyboardInterrupt:
        logging.info("\nПрерывание пользователем. Сохраняем собранные данные...")
        if parser.all_vacancies:
            parser.exporter.export_to_excel(parser.all_vacancies, "agro_vacancies_partial.xlsx")
            logging.info("Частичные данные сохранены в agro_vacancies_partial.xlsx")
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        if parser.all_vacancies:
            parser.exporter.export_to_excel(parser.all_vacancies, "agro_vacancies_error.xlsx")
            logging.info("Собранные данные сохранены в agro_vacancies_error.xlsx")

    logging.info("\nГотово!")


if __name__ == "__main__":
    main()