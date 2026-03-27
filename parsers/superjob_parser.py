# parsers/superjob_parser.py
import requests
import time
import threading
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.salary_processor import SalaryProcessor
from config import SUPERJOB_API_URL, SUPERJOB_API_TOWNS_URL, SUPERJOB_API_KEY, TIMEOUT, MAX_VACANCIES_PER_CITY, \
    MAX_WORKERS, MAX_CONNECTIONS, MAX_CONNECTIONS_PER_HOST

class SuperJobParser:
    def __init__(self):
        self.session = requests.Session()
        # Настройка Retry
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        # Настройка адаптера
        adapter = HTTPAdapter(
            pool_connections=MAX_CONNECTIONS,
            pool_maxsize=MAX_CONNECTIONS,
            max_retries=retry_strategy
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.session.headers.update({
            'X-Api-App-Id': SUPERJOB_API_KEY,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.salary_processor = SalaryProcessor()
        self.all_towns = self._get_all_towns()
        self.semaphore = threading.Semaphore(MAX_CONNECTIONS_PER_HOST)
        print(f"Загружено {len(self.all_towns)} городов для парсинга SuperJob")

    def _is_russian_city(self, city_name: str) -> bool:
        # Список признаков нероссийских городов
        non_russian_indicators = [
            'Kyiv', 'Kiev', 'Minsk', 'Astana', 'Almaty', 'Tashkent',
            'Baku', 'Yerevan', 'Chisinau', 'Vilnius', 'Riga', 'Tallinn',
            'Tbilisi', 'Bishkek', 'Dushanbe', 'Ashgabat', 'Warsaw',
            'Berlin', 'Paris', 'London', 'New York', 'Tokyo', 'Beijing'
        ]
        city_lower = city_name.lower()
        for indicator in non_russian_indicators:
            if indicator.lower() in city_lower:
                return False
        # Если название содержит кириллицу - это российский город
        if any('\u0400' <= char <= '\u04FF' for char in city_name):
            return True
        return False

    def _get_default_towns(self) -> Dict[str, int]:
        #Базовый список городов на случай ошибки API
        return {
            "Москва": 4,
            "Санкт-Петербург": 5,
            "Екатеринбург": 66,
            "Новосибирск": 70,
            "Казань": 88,
            "Нижний Новгород": 61,
            "Самара": 78,
            "Омск": 69,
            "Челябинск": 92,
            "Ростов-на-Дону": 76,
            "Уфа": 85,
            "Красноярск": 58,
            "Пермь": 72,
            "Воронеж": 30,
            "Волгоград": 29,
            "Краснодар": 56,
            "Саратов": 79,
            "Тюмень": 83,
        }

    #Получение всех городов через API SuperJob
    def _get_all_towns(self) -> Dict[str, int]:
        all_towns = {}
        page = 0
        try:
            while True:
                params = {
                    'page': page,
                    'count': 100
                }
                response = self.session.get(SUPERJOB_API_TOWNS_URL, params=params, timeout=TIMEOUT)
                response.raise_for_status()
                data = response.json()
                for town in data.get('objects', []):
                    town_id = town.get('id')
                    town_name = town.get('title')
                    if town_id and town_name:
                        all_towns[town_name] = town_id
                if not data.get('more'):
                    break
                page += 1
                time.sleep(0.2)
        except Exception as e:
            print(f"Ошибка при загрузке городов SuperJob: {e}")
            return self._get_default_towns()
        # Фильтрация только городов России
        russian_towns = {name: town_id for name, town_id in all_towns.items()
                         if self._is_russian_city(name)}
        print(f"Найдено {len(russian_towns)} российских городов")
        return russian_towns
    #Поиск вакансий на SuperJob по всем городам
    def search_vacancies(self, profession_name: str) -> List[Dict]:
        all_vacancies = []
        towns_to_parse = list(self.all_towns.items())[:100]
        max_sj_workers = min(MAX_WORKERS, 5)
        with ThreadPoolExecutor(max_workers=max_sj_workers) as executor:
            futures = {}
            for city_name, town_id in towns_to_parse:
                future = executor.submit(self._search_in_city, profession_name, town_id, city_name)
                futures[future] = city_name
            for future in as_completed(futures):
                city_name = futures[future]
                try:
                    vacancies = future.result()
                    all_vacancies.extend(vacancies)
                    if vacancies:
                        print(f"SuperJob в {city_name}: найдено {len(vacancies)} вакансий")
                    time.sleep(0.1)
                except Exception as e:
                    print(f"Ошибка в городе {city_name}: {e}")
        return all_vacancies

    #Поиск вакансий в конкретном городе
    def _search_in_city(self, profession_name: str, town_id: int, city_name: str) -> List[Dict]:
        vacancies = []
        page = 0
        max_pages = 1
        with self.semaphore:
            while page < max_pages and len(vacancies) < MAX_VACANCIES_PER_CITY:
                params = {
                    'keyword': profession_name,
                    'town': town_id,
                    'count': 20,
                    'page': page
                }
                try:
                    response = self.session.get(SUPERJOB_API_URL, params=params, timeout=TIMEOUT)
                    response.raise_for_status()
                    data = response.json()
                    for item in data.get('objects', []):
                        vacancy_data = self._parse_vacancy(item, profession_name, city_name)
                        if vacancy_data:
                            vacancies.append(vacancy_data)
                    if not data.get('more'):
                        break
                    page += 1
                    time.sleep(0.3)
                except Exception as e:
                    break
        return vacancies
    #Парсинг одной вакансии
    def _parse_vacancy(self, vacancy: Dict, search_term: str, city_name: str = None) -> Optional[Dict]:
        try:
            salary_from = vacancy.get('payment_from')
            salary_to = vacancy.get('payment_to')
            currency = vacancy.get('currency')
            salary_from, salary_to, currency = self.salary_processor.parse_salary_superjob(
                salary_from, salary_to, currency
            )
            average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            city = city_name or vacancy.get('town', {}).get('title', 'Не указан')
            return {
                'profession_code': self._get_profession_code(search_term),
                'profession_name': search_term,
                'title': vacancy.get('profession', ''),
                'city': city,
                'salary_from': salary_from,
                'salary_to': salary_to,
                'salary_average': average_salary,
                'currency': currency or 'RUB',
                'source': 'superjob.ru',
                'url': vacancy.get('link', ''),
                'company': vacancy.get('firm_name', ''),
                'experience': vacancy.get('experience', {}).get('title', ''),
                'employment': vacancy.get('type_of_work', {}).get('title', '')
            }
        except Exception as e:
            return None

    def _get_profession_code(self, profession_name: str) -> str:
        from professions import PROFESSIONS
        for code, name in PROFESSIONS.items():
            if profession_name.lower() in name.lower():
                return code
        return "unknown"