import requests
import time
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from utils.salary_processor import SalaryProcessor
from config import TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION


class YavagroParser:
    def __init__(self):
        self.base_url = "https://yavagro.ru"
        self.vacancies_url = f"{self.base_url}/vacancies"
        self.salary_processor = SalaryProcessor()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection': 'keep-alive',
        })

    def search_vacancies(self, profession_name: str) -> List[Dict]:
        vacancies = []

        try:
            search_url = f"{self.vacancies_url}?search={profession_name}"
            print(f"Запрос к yavagro.ru: {search_url}")

            response = self.session.get(search_url, timeout=TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            vacancy_elements = self._find_vacancy_elements(soup)

            for element in vacancy_elements[:MAX_VACANCIES_PER_PROFESSION]:
                vacancy_data = self._parse_vacancy(element, profession_name)
                if vacancy_data:
                    vacancies.append(vacancy_data)

            #Пагинация
            page = 2
            while len(vacancies) < MAX_VACANCIES_PER_PROFESSION:
                next_url = f"{search_url}&page={page}"
                response = self.session.get(next_url, timeout=TIMEOUT)

                if response.status_code != 200:
                    break
                soup = BeautifulSoup(response.text, 'html.parser')
                new_elements = self._find_vacancy_elements(soup)
                if not new_elements:
                    break
                for element in new_elements:
                    if len(vacancies) >= MAX_VACANCIES_PER_PROFESSION:
                        break
                    vacancy_data = self._parse_vacancy(element, profession_name)
                    if vacancy_data:
                        vacancies.append(vacancy_data)
                page += 1
                time.sleep(0.3)
        except Exception as e:
            print(f"Ошибка при парсинге yavagro.ru: {e}")
        return vacancies

    def _find_vacancy_elements(self, soup):
        selectors = [
            'div.vacancy-item',
            'div.job-item',
            'div.vacancy-card',
            'div.vacancy',
            'article.vacancy',
            'div[class*="vacancy"]',
            'div[class*="job"]',
            'div.item',
            'div.card'
        ]

        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                return elements

        all_links = soup.find_all('a', href=re.compile(r'/vacancy/\d+'))
        if all_links:
            parent_elements = [link.parent for link in all_links[:20] if link.parent]
            if parent_elements:
                return parent_elements

        return []

    def _parse_vacancy(self, element, search_term: str) -> Optional[Dict]:
        try:
            #Заголовок
            title = self._extract_text(element, ['h3', 'h2', '.title', '.vacancy-title', '.job-title', 'a'],
                                       "Не указано")

            #Город
            city = self._extract_text(element, ['.city', '.location', '.town', '.place', '[class*="city"]',
                                                '[class*="location"]'], "Не указан")
            city = self._clean_city_name(city)

            #Зарплата
            salary_text = self._extract_text(element, ['.salary', '.price', '[class*="salary"]', '[class*="price"]'],
                                             "")

            #Компания
            company = self._extract_text(element, ['.company', '.employer', '.firm', '[class*="company"]'], "")

            #URL
            url = self._extract_url(element)

            salary_from, salary_to, currency = self.salary_processor.parse_salary_text(salary_text)
            average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            return {
                'profession_code': self._get_profession_code(search_term),
                'profession_name': search_term,
                'title': title,
                'city': city,
                'salary_from': salary_from,
                'salary_to': salary_to,
                'salary_average': average_salary,
                'currency': currency or 'RUB',
                'source': 'yavagro.ru',
                'url': url,
                'company': company,
                'experience': '',
                'employment': ''
            }

        except Exception as e:
            return None

    def _extract_text(self, element, selectors, default=""):
        for selector in selectors:
            elem = element.select_one(selector)
            if elem:
                text = elem.text.strip()
                if text:
                    return text
        return default

    def _extract_url(self, element) -> str:
        url_selectors = ['a', '.title a', '.vacancy-title a', '.job-title a', 'h3 a', 'h2 a']

        for selector in url_selectors:
            url_elem = element.select_one(selector)
            if url_elem and url_elem.get('href'):
                url = url_elem.get('href')
                if not url.startswith('http'):
                    url = self.base_url + url
                return url

        return ""

    def _clean_city_name(self, city: str) -> str:
        city = city.replace('г.', '').replace('г ', '').strip()
        city = re.sub(r'\([^)]*\)', '', city).strip()
        city = re.sub(r'\s+', ' ', city)
        return city

    def _get_profession_code(self, profession_name: str) -> str:
        from professions import PROFESSIONS

        for code, name in PROFESSIONS.items():
            if profession_name.lower() in name.lower():
                return code
        return "unknown"