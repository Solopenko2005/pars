import requests
import time
import threading
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.salary_processor import SalaryProcessor
from config import TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION, MAX_WORKERS, MAX_CONNECTIONS, MAX_CONNECTIONS_PER_HOST

class HHParser:
    def __init__(self):
        # Создание сессии с настройками пула соединений
        self.session = requests.Session()
        # Настройка Retry стратегии
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.salary_processor = SalaryProcessor()
        self.all_regions = self._get_all_regions()
        print(f"Загружено {len(self.all_regions)} регионов для парсинга hh.ru")
        self.semaphore = threading.Semaphore(MAX_CONNECTIONS_PER_HOST)

    #ID регионов
    def _get_all_regions(self) -> List[int]:
        try:
            response = self.session.get("https://api.hh.ru/areas", timeout=TIMEOUT)
            response.raise_for_status()
            areas = response.json()
            russia = None
            for area in areas:
                if area['name'] == 'Россия':
                    russia = area
                    break
            if not russia:
                return [1]
            region_ids = []
            for region in russia.get('areas', []):
                region_ids.append(region['id'])
                for city in region.get('areas', []):
                    region_ids.append(city['id'])
            return region_ids
        except Exception as e:
            print(f"Ошибка при загрузке регионов: {e}")
            return [1]

    #Поиск вакансий
    def search_vacancies(self, profession_name: str) -> List[Dict]:
        all_vacancies = []
        # Ограничение количества потоков для hh
        max_hh_workers = min(MAX_WORKERS // 2, 5)
        with ThreadPoolExecutor(max_workers=max_hh_workers) as executor:
            futures = {}
            for region_id in self.all_regions[:500]:
                future = executor.submit(self._search_in_region, profession_name, region_id)
                futures[future] = region_id
            for future in as_completed(futures):
                region_id = futures[future]
                try:
                    vacancies = future.result()
                    all_vacancies.extend(vacancies)
                    if vacancies:
                        print(f"hh.ru в регионе {region_id}: найдено {len(vacancies)} вакансий")
                    time.sleep(0.1)
                except Exception as e:
                    print(f"Ошибка в регионе {region_id}: {e}")
        return all_vacancies[:MAX_VACANCIES_PER_PROFESSION]

    def _search_in_region(self, profession_name: str, region_id: int) -> List[Dict]:
        vacancies = []
        page = 0
        max_pages = 1

        with self.semaphore:
            while page < max_pages:
                params = {
                    'text': profession_name,
                    'area': region_id,
                    'per_page': 50,
                    'page': page,
                    'only_with_salary': False
                }
                try:
                    response = self.session.get(HH_API_URL, params=params, timeout=TIMEOUT)
                    response.raise_for_status()
                    data = response.json()
                    for item in data.get('items', []):
                        vacancy_data = self._parse_vacancy(item, profession_name)
                        vacancies.append(vacancy_data)
                    if page >= data.get('pages', 0) - 1:
                        break
                    page += 1
                    time.sleep(0.3)
                except Exception as e:
                    break
        return vacancies

    #Парсинг вакансии
    def _parse_vacancy(self, vacancy: Dict, search_term: str) -> Dict:
        salary_from, salary_to, currency = self.salary_processor.parse_salary_hh(
            vacancy.get('salary')
        )

        average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

        return {
            'profession_code': self._get_profession_code(search_term),
            'profession_name': search_term,
            'title': vacancy.get('name', ''),
            'city': vacancy.get('area', {}).get('name', 'Не указан'),
            'salary_from': salary_from,
            'salary_to': salary_to,
            'salary_average': average_salary,
            'currency': currency or 'RUB',
            'source': 'hh.ru',
            'url': vacancy.get('alternate_url', ''),
            'company': vacancy.get('employer', {}).get('name', ''),
            'experience': vacancy.get('experience', {}).get('name', ''),
            'employment': vacancy.get('employment', {}).get('name', '')
        }

    #Код профессии
    def _get_profession_code(self, profession_name: str) -> str:
        from professions import PROFESSIONS

        for code, name in PROFESSIONS.items():
            if profession_name.lower() in name.lower():
                return code
        return "unknown"