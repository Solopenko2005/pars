import requests
import time
import re
from typing import List, Dict
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from utils.salary_processor import SalaryProcessor
from config import TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION


class YavagroParser:
    def __init__(self):
        self.base_url = "https://yavagro.ru"
        self.vacancies_url = f"{self.base_url}/vacancies"
        self.salary_processor = SalaryProcessor()
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def search_vacancies(self, profession_name: str) -> List[Dict]:
        vacancies = []
        self._init_driver()

        try:
            search_url = f"{self.vacancies_url}?search={profession_name}"
            print(f"Запрос к yavagro.ru: {search_url}")

            self.driver.get(search_url)
            time.sleep(3)
            self._scroll_page()

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            vacancy_elements = self._find_vacancy_elements(soup)

            for element in vacancy_elements[:MAX_VACANCIES_PER_PROFESSION]:
                vacancy_data = self._parse_vacancy(element, profession_name)
                if vacancy_data:
                    vacancies.append(vacancy_data)

            page = 2
            while len(vacancies) < MAX_VACANCIES_PER_PROFESSION and self._has_next_page(soup):
                next_url = f"{search_url}&page={page}"
                self.driver.get(next_url)
                time.sleep(2)

                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                new_elements = self._find_vacancy_elements(soup)

                for element in new_elements:
                    if len(vacancies) >= MAX_VACANCIES_PER_PROFESSION:
                        break
                    vacancy_data = self._parse_vacancy(element, profession_name)
                    if vacancy_data:
                        vacancies.append(vacancy_data)

                page += 1

        except Exception as e:
            print(f"Ошибка при парсинге yavagro.ru: {e}")
        finally:
            self._close_driver()

        return vacancies

    def _init_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def _close_driver(self):
        if self.driver:
            self.driver.quit()

    def _scroll_page(self):
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def _find_vacancy_elements(self, soup):
        selectors = [
            'div.vacancy-item',
            'div.job-item',
            'div.vacancy-card',
            'div.vacancy',
            'article.vacancy',
            'div[class*="vacancy"]',
            'div[class*="job"]'
        ]

        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                return elements

        # Если не нашли по селекторам, ищем по ссылкам
        all_links = soup.find_all('a', href=re.compile(r'/vacancy/\d+'))
        if all_links:
            parent_elements = [link.parent for link in all_links[:20] if link.parent]
            if parent_elements:
                return parent_elements

        return []

    def _parse_vacancy(self, element, search_term: str) -> Dict:
        try:
            # Заголовок
            title_selectors = ['h3', 'h2', '.title', '.vacancy-title', '.job-title', 'a']
            title = "Не указано"
            for selector in title_selectors:
                title_elem = element.select_one(selector)
                if title_elem:
                    title = title_elem.text.strip()
                    break

            # Город
            city_selectors = ['.city', '.location', '.town', '.place', '[class*="city"]', '[class*="location"]']
            city = "Не указан"
            for selector in city_selectors:
                city_elem = element.select_one(selector)
                if city_elem:
                    city = city_elem.text.strip()
                    break

            # Зарплата
            salary_selectors = ['.salary', '.price', '[class*="salary"]', '[class*="price"]']
            salary_text = ""
            for selector in salary_selectors:
                salary_elem = element.select_one(selector)
                if salary_elem:
                    salary_text = salary_elem.text.strip()
                    break

            # Компания
            company_selectors = ['.company', '.employer', '.firm', '[class*="company"]']
            company = ""
            for selector in company_selectors:
                company_elem = element.select_one(selector)
                if company_elem:
                    company = company_elem.text.strip()
                    break

            # URL
            url_selectors = ['a', '.title a', '.vacancy-title a']
            url = ""
            for selector in url_selectors:
                url_elem = element.select_one(selector)
                if url_elem and url_elem.get('href'):
                    url = url_elem.get('href')
                    if not url.startswith('http'):
                        url = self.base_url + url
                    break

            salary_from, salary_to, currency = self.salary_processor.parse_salary_text(salary_text)
            average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            city = self._clean_city_name(city)

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
            print(f"      Ошибка при парсинге вакансии yavagro.ru: {e}")
            return None

    def _clean_city_name(self, city: str) -> str:
        city = city.replace('г.', '').replace('г ', '').strip()
        import re
        city = re.sub(r'\([^)]*\)', '', city).strip()
        return city

    def _has_next_page(self, soup) -> bool:
        pagination_selectors = [
            '.pagination .next',
            '.pagination a:contains("Следующая")',
            'a[rel="next"]',
            '.next-page'
        ]

        for selector in pagination_selectors:
            next_elem = soup.select_one(selector)
            if next_elem:
                return True

        return False

    def _get_profession_code(self, profession_name: str) -> str:
        from professions import PROFESSIONS

        for code, name in PROFESSIONS.items():
            if profession_name.lower() in name.lower():
                return code
        return "unknown"