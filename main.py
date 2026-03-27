# main.py
import sys
import io
import time
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from professions import SEARCH_TERMS, PROFESSIONS
from parsers.hh_parser import HHParser
from parsers.superjob_parser import SuperJobParser
from parsers.yavagro_parser import YavagroParser
from parsers.svoevagro_parser import SvoevagroParser
from utils.data_exporter import DataExporter
from config import DELAY, PARSE_ALL_RUSSIA, MAX_WORKERS

#Настройка кодировки для Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

#Настройка логирования с UTF-8
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'parser_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


#Счетчик потоков
class ThreadSafeCounter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self._value += 1
            return self._value

    def get_value(self):
        with self._lock:
            return self._value


class AgroVacancyParser:
    def __init__(self):
        self.hh_parser = HHParser()
        self.superjob_parser = SuperJobParser()
        self.yavagro_parser = YavagroParser()
        self.svoevagro_parser = SvoevagroParser()
        self.exporter = DataExporter()
        self.all_vacancies = []
        self.vacancies_lock = threading.Lock()
        self.counter = ThreadSafeCounter()

        logging.info(
            f"Инициализация парсера. Режим: {'все города России' if PARSE_ALL_RUSSIA else 'выборочные города'}"
        )

    #Парсинг всех профессий
    def parse_all_professions(self):
        total_professions = len(SEARCH_TERMS)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}

            #Параллельный парсинг по всем профессиям
            for idx, profession in enumerate(SEARCH_TERMS, 1):
                future = executor.submit(self._parse_single_profession, profession, idx, total_professions)
                futures[future] = profession

            for future in as_completed(futures):
                profession = futures[future]
                try:
                    vacancies = future.result()
                    with self.vacancies_lock:
                        self.all_vacancies.extend(vacancies)
                    logging.info(f"[OK] Профессия '{profession}': собрано {len(vacancies)} вакансий")
                except Exception as e:
                    logging.error(f"[ERROR] Ошибка при парсинге профессии '{profession}': {e}")

    #Парсинг одной профессий
    def _parse_single_profession(self, profession: str, idx: int, total: int) -> List[Dict]:
        logging.info(f"[{idx}/{total}] Начинаем парсинг профессии: {profession}")
        start_time = time.time()

        all_vacancies = []

        with ThreadPoolExecutor(max_workers=4) as executor:
            sources = [
                ("HH.ru", self.hh_parser.search_vacancies),
                ("SuperJob", self.superjob_parser.search_vacancies),
                ("Yavagro", self.yavagro_parser.search_vacancies),
                ("Svoevagro", self.svoevagro_parser.search_vacancies)
            ]

            futures = {}
            for source_name, parse_func in sources:
                future = executor.submit(self._parse_source_safe, profession, source_name, parse_func)
                futures[future] = source_name

            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    vacancies = future.result()
                    all_vacancies.extend(vacancies)
                    logging.debug(f"  [{profession}] {source_name}: {len(vacancies)} вакансий")
                except Exception as e:
                    logging.error(f"  [{profession}] Ошибка в {source_name}: {e}")

        elapsed_time = time.time() - start_time
        logging.info(
            f"[{idx}/{total}] Профессия '{profession}': найдено {len(all_vacancies)} вакансий, время: {elapsed_time:.2f} сек")

        return all_vacancies

    #Парсинг с задержкой
    def _parse_source_safe(self, profession: str, source_name: str, parse_func):
        try:
            time.sleep(self.counter.increment() * 0.1)
            return parse_func(profession)
        except Exception as e:
            logging.error(f"Ошибка в {source_name} для профессии {profession}: {e}")
            return []

    def get_statistics(self):
        if not self.all_vacancies:
            logging.warning("Нет данных для статистики")
            return
        total_vacancies = len(self.all_vacancies)
        logging.info(f"Всего вакансий собрано: {total_vacancies}")

        #Статистика по источникам
        sources = {}
        for vac in self.all_vacancies:
            source = vac['source']
            sources[source] = sources.get(source, 0) + 1

        logging.info("\nПо источникам:")
        for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
            logging.info(f"{source}: {count} вакансий ({count / total_vacancies * 100:.1f}%)")

        #Статистика по профессиям
        professions_count = {}
        for vac in self.all_vacancies:
            prof = vac['profession_name']
            professions_count[prof] = professions_count.get(prof, 0) + 1

        logging.info("\nТоп-10 профессий по количеству вакансий:")
        sorted_profs = sorted(professions_count.items(), key=lambda x: x[1], reverse=True)[:10]
        for prof, count in sorted_profs:
            logging.info(f"{prof}: {count} вакансий")

        #Статистика по городам
        cities_count = {}
        for vac in self.all_vacancies:
            city = vac['city']
            if city != 'Не указан':
                cities_count[city] = cities_count.get(city, 0) + 1

        logging.info("\nТоп-20 городов по количеству вакансий:")
        sorted_cities = sorted(cities_count.items(), key=lambda x: x[1], reverse=True)[:20]
        for city, count in sorted_cities:
            logging.info(f"  {city}: {count} вакансий")

        logging.info(f"\nВсего городов: {len(cities_count)}")

        #Статистика по зарплатам
        salaries = [v['salary_average'] for v in self.all_vacancies if v.get('salary_average')]
        if salaries:
            avg_salary = sum(salaries) / len(salaries)
            max_salary = max(salaries)
            min_salary = min(salaries)
            logging.info(f"\nСтатистика по зарплатам:")
            logging.info(f"Средняя зарплата: {avg_salary:,.0f} руб.")
            logging.info(f"Максимальная: {max_salary:,.0f} руб.")
            logging.info(f"Минимальная: {min_salary:,.0f} руб.")

            salary_ranges = {
                "до 30 000": 0,
                "30 000 - 50 000": 0,
                "50 000 - 80 000": 0,
                "80 000 - 120 000": 0,
                "более 120 000": 0
            }

            for salary in salaries:
                if salary < 30000:
                    salary_ranges["до 30 000"] += 1
                elif salary < 50000:
                    salary_ranges["30 000 - 50 000"] += 1
                elif salary < 80000:
                    salary_ranges["50 000 - 80 000"] += 1
                elif salary < 120000:
                    salary_ranges["80 000 - 120 000"] += 1
                else:
                    salary_ranges["более 120 000"] += 1

            logging.info(f"\nРаспределение по зарплатным диапазонам:")
            for range_name, count in salary_ranges.items():
                percentage = (count / len(salaries)) * 100
                logging.info(f"  {range_name}: {count} вакансий ({percentage:.1f}%)")

    def get_top_cities(self, limit: int = 20) -> List[Tuple[str, int]]:
        cities_count = {}
        for vac in self.all_vacancies:
            city = vac['city']
            if city != 'Не указан':
                cities_count[city] = cities_count.get(city, 0) + 1

        return sorted(cities_count.items(), key=lambda x: x[1], reverse=True)[:limit]


def main():
    parser = AgroVacancyParser()
    try:
        start_time = time.time()
        parser.parse_all_professions()
        elapsed_time = time.time() - start_time

        parser.get_statistics()

        logging.info(f"\nОбщее время выполнения: {elapsed_time / 60:.2f} минут")
        if parser.all_vacancies:
            logging.info(f"Средняя скорость: {len(parser.all_vacancies) / elapsed_time:.1f} вакансий/сек")

        if parser.all_vacancies:
            logging.info("\nСохраняем результаты...")
            excel_file = parser.exporter.export_to_excel(parser.all_vacancies)
            csv_file = parser.exporter.export_to_csv(parser.all_vacancies)

            logging.info(f"\nРезультаты сохранены:")
            logging.info(f"Excel: {excel_file}")
            logging.info(f"CSV: {csv_file}")
            logging.info(f"Всего записей: {len(parser.all_vacancies)}")

            top_cities = parser.get_top_cities(5)
            logging.info(f"\nТоп-5 городов по количеству вакансий:")
            for city, count in top_cities:
                logging.info(f"  {city}: {count} вакансий")
        else:
            logging.warning("\nВакансии не найдены. Проверьте настройки парсинга.")

    except KeyboardInterrupt:
        logging.info("\n[!] Прерывание пользователем. Сохраняем собранные данные...")
        if parser.all_vacancies:
            parser.exporter.export_to_excel(parser.all_vacancies, "agro_vacancies_partial.xlsx")
            logging.info("Частичные данные сохранены в agro_vacancies_partial.xlsx")
    except Exception as e:
        logging.error(f"[ERROR] Критическая ошибка: {e}")
        import traceback
        logging.error(traceback.format_exc())
        if parser.all_vacancies:
            parser.exporter.export_to_excel(parser.all_vacancies, "agro_vacancies_error.xlsx")
            logging.info("Собранные данные сохранены в agro_vacancies_error.xlsx")

    logging.info("\n[OK] Готово!")


if __name__ == "__main__":
    main()