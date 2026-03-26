import requests
import time
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from utils.salary_processor import SalaryProcessor
from config import TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION


class YavagroParser:
    def __init__(self, use_selenium: bool = True):
        self.base_url = "https://yavagro.ru"
        self.vacancies_url = f"{self.base_url}/vacancies"
        self.salary_processor = SalaryProcessor()
        self.use_selenium = use_selenium
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

    def search_vacancies(self, profession_name: str) -> List[Dict]:
        vacancies = []

        try:
            if self.use_selenium:
                vacancies = self._search_with_selenium(profession_name)
            else:
                vacancies = self._search_with_requests(profession_name)

        except Exception as e:
            print(f"Ошибка при парсинге yavagro.ru: {e}")

        return vacancies

    def _search_with_selenium(self, profession_name: str) -> List[Dict]:
        vacancies = []
        self._init_driver()

        try:
            search_url = f"{self.vacancies_url}?search={profession_name}"
            print(f"Запрос к yavagro.ru (Selenium): {search_url}")

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
            print(f"      Ошибка в Selenium парсинге: {e}")
        finally:
            self._close_driver()

        return vacancies

    def _search_with_requests(self, profession_name: str) -> List[Dict]:
        vacancies = []

        try:
            # Формируем URL для поиска
            search_url = f"{self.vacancies_url}?search={profession_name}"
            print(f"    Запрос к yavagro.ru (Requests): {search_url}")

            response = self.session.get(search_url, timeout=TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            vacancy_elements = self._find_vacancy_elements(soup)

            for element in vacancy_elements[:MAX_VACANCIES_PER_PROFESSION]:
                vacancy_data = self._parse_vacancy(element, profession_name)
                if vacancy_data:
                    vacancies.append(vacancy_data)

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
                time.sleep(DELAY)

        except Exception as e:
            print(f"      Ошибка в Requests парсинге: {e}")

        return vacancies

    def _init_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')  # Без GUI
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def _close_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    def _scroll_page(self):
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        scroll_attempts = 0
        max_scroll_attempts = 10

        while scroll_attempts < max_scroll_attempts:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            scroll_attempts += 1

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
            'div.card',
            'li.vacancy',
            '.vacancies-list > div',
            '.jobs-list > div'
        ]

        for selector in selectors:
            elements = soup.select(selector)
            if elements and len(elements) > 0:
                print(f"      Найдены вакансии по селектору: {selector} ({len(elements)} шт.)")
                return elements

        all_links = soup.find_all('a', href=re.compile(r'/vacancy/\d+'))
        if all_links:
            parent_elements = [link.parent for link in all_links[:20] if link.parent]
            if parent_elements:
                print(f"Найдены вакансии по ссылкам: {len(parent_elements)} шт.")
                return parent_elements

        print("Вакансии не найдены на странице")
        return []

    def _parse_vacancy(self, element, search_term: str) -> Optional[Dict]:
        try:
            title = self._extract_title(element)

            city = self._extract_city(element)

            salary_text = self._extract_salary(element)
            salary_from, salary_to, currency = self.salary_processor.parse_salary_text(salary_text)
            average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            company = self._extract_company(element)

            url = self._extract_url(element)

            experience, employment = self._extract_requirements(element)

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
                'experience': experience,
                'employment': employment
            }

        except Exception as e:
            print(f"Ошибка при парсинге вакансии yavagro.ru: {e}")
            return None

    def _extract_title(self, element) -> str:
        title_selectors = [
            'h3',
            'h2',
            '.vacancy-title',
            '.job-title',
            '.title',
            'a.vacancy-name',
            '.vacancy-name',
            '[class*="title"]',
            '[class*="name"]'
        ]

        for selector in title_selectors:
            title_elem = element.select_one(selector)
            if title_elem:
                title = title_elem.text.strip()
                if title:
                    return title

        link = element.find('a')
        if link and link.text.strip():
            return link.text.strip()

        return "Не указано"

    def _extract_city(self, element) -> str:
        city_selectors = [
            '.city',
            '.location',
            '.town',
            '.place',
            '[class*="city"]',
            '[class*="location"]',
            '.address',
            '.region'
        ]

        for selector in city_selectors:
            city_elem = element.select_one(selector)
            if city_elem:
                city = city_elem.text.strip()
                if city:
                    return self._clean_city_name(city)

        text = element.get_text()
        city_pattern = r'(?:г\.?\s*)?([А-Я][а-я]+(?:-[А-Я][а-я]+)?)\s*(?:\([^)]*\))?'
        match = re.search(city_pattern, text)
        if match:
            return match.group(1)

        return "Не указан"

    def _extract_salary(self, element) -> str:
        salary_selectors = [
            '.salary',
            '.price',
            '[class*="salary"]',
            '[class*="price"]',
            '.wage',
            '.payment'
        ]

        for selector in salary_selectors:
            salary_elem = element.select_one(selector)
            if salary_elem:
                salary_text = salary_elem.text.strip()
                if salary_text:
                    return salary_text

        text = element.get_text()
        salary_patterns = [
            r'(\d[\d\s]*)\s*(?:руб|₽|р\.)',
            r'(?:от|до)\s*(\d[\d\s]*)\s*(?:руб|₽|р\.)',
            r'(\d[\d\s]*)\s*(?:-\s*\d[\d\s]*)?\s*(?:руб|₽|р\.)'
        ]

        for pattern in salary_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(0)

        return ""

    def _extract_company(self, element) -> str:
        company_selectors = [
            '.company',
            '.employer',
            '.firm',
            '[class*="company"]',
            '[class*="employer"]',
            '.org-name'
        ]

        for selector in company_selectors:
            company_elem = element.select_one(selector)
            if company_elem:
                company = company_elem.text.strip()
                if company:
                    return company

        return ""

    def _extract_url(self, element) -> str:
        url_selectors = [
            'a',
            '.vacancy-title a',
            '.job-title a',
            'h3 a',
            'h2 a'
        ]

        for selector in url_selectors:
            url_elem = element.select_one(selector)
            if url_elem and url_elem.get('href'):
                url = url_elem.get('href')
                if url.startswith('/'):
                    url = self.base_url + url
                return url

        return ""

    def _extract_requirements(self, element):
        experience = ""
        employment = ""

        exp_selectors = [
            '.experience',
            '[class*="exp"]',
            '.work-experience'
        ]

        for selector in exp_selectors:
            exp_elem = element.select_one(selector)
            if exp_elem:
                experience = exp_elem.text.strip()
                break

        emp_selectors = [
            '.employment',
            '.schedule',
            '[class*="employment"]',
            '[class*="schedule"]'
        ]

        for selector in emp_selectors:
            emp_elem = element.select_one(selector)
            if emp_elem:
                employment = emp_elem.text.strip()
                break

        return experience, employment

    def _clean_city_name(self, city: str) -> str:
        city = city.replace('г.', '').replace('г ', '').strip()
        city = city.replace('город', '').strip()
        city = re.sub(r'\([^)]*\)', '', city).strip()
        city = re.sub(r'\s+', ' ', city)
        return city

    def _has_next_page(self, soup) -> bool:
        pagination_selectors = [
            '.pagination .next',
            '.pagination a:contains("Следующая")',
            'a[rel="next"]',
            '.next-page',
            '.pagination .page-item:last-child a',
            'a:contains("→")',
            'a:contains("»")'
        ]

        for selector in pagination_selectors:
            next_elem = soup.select_one(selector)
            if next_elem:
                return True

        pagination = soup.select('.pagination a')
        if pagination:
            last_link = pagination[-1]
            if last_link.text.strip() in ['→', '»', 'Следующая', 'Next']:
                return True

        return False

    def _get_profession_code(self, profession_name: str) -> str:
        from professions import PROFESSIONS

        for code, name in PROFESSIONS.items():
            if profession_name.lower() in name.lower():
                return code
        return "unknown"

    def get_vacancy_details(self, url: str) -> Optional[Dict]:
        try:
            if self.use_selenium:
                self._init_driver()
                self.driver.get(url)
                time.sleep(2)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                self._close_driver()
            else:
                response = self.session.get(url, timeout=TIMEOUT)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

            details = {
                'description': self._extract_description(soup),
                'requirements': self._extract_requirements_text(soup),
                'conditions': self._extract_conditions(soup),
                'contacts': self._extract_contacts(soup)
            }

            return details

        except Exception as e:
            print(f"Ошибка при получении деталей вакансии: {e}")
            return None

    def _extract_description(self, soup) -> str:
        desc_selectors = [
            '.vacancy-description',
            '.job-description',
            '.description',
            '[class*="description"]',
            '.vacancy-text'
        ]

        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                return desc_elem.text.strip()

        return ""

    def _extract_requirements_text(self, soup) -> str:
        req_selectors = [
            '.requirements',
            '.vacancy-requirements',
            '[class*="requirement"]'
        ]

        for selector in req_selectors:
            req_elem = soup.select_one(selector)
            if req_elem:
                return req_elem.text.strip()

        return ""

    def _extract_conditions(self, soup) -> str:
        cond_selectors = [
            '.conditions',
            '.vacancy-conditions',
            '[class*="condition"]'
        ]

        for selector in cond_selectors:
            cond_elem = soup.select_one(selector)
            if cond_elem:
                return cond_elem.text.strip()

        return ""

    def _extract_contacts(self, soup) -> str:
        contact_selectors = [
            '.contacts',
            '.vacancy-contacts',
            '[class*="contact"]',
            '.company-info'
        ]

        for selector in contact_selectors:
            contact_elem = soup.select_one(selector)
            if contact_elem:
                return contact_elem.text.strip()

        return ""