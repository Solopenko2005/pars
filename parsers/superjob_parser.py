# parsers/superjob_parser.py
import requests
import time
from typing import List, Dict
from utils.salary_processor import SalaryProcessor
from config import SUPERJOB_API_URL, SUPERJOB_API_KEY, TIMEOUT, DELAY, MAX_VACANCIES_PER_CITY, SUPERJOB_TOWNS


class SuperJobParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'X-Api-App-Id': SUPERJOB_API_KEY,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.salary_processor = SalaryProcessor()

    def search_vacancies(self, profession_name: str) -> List[Dict]:
        all_vacancies = []

        for city_name, town_id in SUPERJOB_TOWNS.items():
            print(f"Парсинг SuperJob в городе {city_name}...")
            vacancies = self._search_in_city(profession_name, town_id, city_name)
            all_vacancies.extend(vacancies)
            time.sleep(DELAY)

            if len(all_vacancies) >= MAX_VACANCIES_PER_CITY * len(SUPERJOB_TOWNS):
                break

        return all_vacancies

    def _search_in_city(self, profession_name: str, town_id: int, city_name: str) -> List[Dict]:
        vacancies = []
        page = 0

        while len(vacancies) < MAX_VACANCIES_PER_CITY:
            params = {
                'keyword': profession_name,
                'town': town_id,
                'count': 100,
                'page': page
            }

            try:
                response = self.session.get(SUPERJOB_API_URL, params=params, timeout=TIMEOUT)
                response.raise_for_status()
                data = response.json()

                for item in data.get('objects', []):
                    vacancy_data = self._parse_vacancy(item, profession_name, city_name)
                    vacancies.append(vacancy_data)

                if not data.get('more'):
                    break

                page += 1
                time.sleep(DELAY)

            except Exception as e:
                print(f"    Ошибка при парсинге SuperJob в городе {city_name}: {e}")
                break

        return vacancies

    def _parse_vacancy(self, vacancy: Dict, search_term: str, city_name: str = None) -> Dict:
        salary_from = vacancy.get('payment_from')
        salary_to = vacancy.get('payment_to')
        currency = vacancy.get('currency')

        salary_from, salary_to, currency = self.salary_processor.parse_salary_superjob(
            salary_from, salary_to, currency
        )

        average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

        # Используем переданное название города или из вакансии
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

    def _get_profession_code(self, profession_name: str) -> str:
        from professions import PROFESSIONS

        for code, name in PROFESSIONS.items():
            if profession_name.lower() in name.lower():
                return code
        return "unknown"