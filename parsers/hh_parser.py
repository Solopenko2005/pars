# parsers/hh_parser.py
import requests
import time
from typing import List, Dict
from utils.salary_processor import SalaryProcessor
from config import HH_API_URL, TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION, PARSE_ALL_RUSSIA, AREAS_TO_PARSE

class HHParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.salary_processor = SalaryProcessor()
        self.regions = self._get_all_regions() if PARSE_ALL_RUSSIA else AREAS_TO_PARSE

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
                return [1] #ID России

            #ID всех регионов
            region_ids = []
            for region in russia.get('areas', []):
                region_ids.append(region['id'])
                for city in region.get('areas', []):
                    region_ids.append(city['id'])

            print(f"Загружено {len(region_ids)} регионов России для парсинга")
            return region_ids

        except Exception as e:
            print(f"Ошибка при загрузке регионов: {e}")
            return [1]

    def search_vacancies(self, profession_name: str, region_id: int = None) -> List[Dict]:
        if region_id:
            return self._search_in_region(profession_name, region_id)
        else:
            #Парсинг по всем регионам
            all_vacancies = []
            for region in self.regions:
                print(f"Парсинг HH.ru в регионе ID {region}...")
                vacancies = self._search_in_region(profession_name, region)
                all_vacancies.extend(vacancies)
                time.sleep(DELAY)

                if len(all_vacancies) >= MAX_VACANCIES_PER_PROFESSION:
                    break

            return all_vacancies[:MAX_VACANCIES_PER_PROFESSION]

    def _search_in_region(self, profession_name: str, region_id: int) -> List[Dict]:
        vacancies = []
        page = 0

        while len(vacancies) < MAX_VACANCIES_PER_PROFESSION // len(self.regions) + 10:
            params = {
                'text': profession_name,
                'area': region_id,
                'per_page': 100,
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
                time.sleep(DELAY)

            except Exception as e:
                print(f"    Ошибка при парсинге HH.ru в регионе {region_id}: {e}")
                break

        return vacancies

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

    def _get_profession_code(self, profession_name: str) -> str:
        from professions import PROFESSIONS

        for code, name in PROFESSIONS.items():
            if profession_name.lower() in name.lower():
                return code
        return "unknown"