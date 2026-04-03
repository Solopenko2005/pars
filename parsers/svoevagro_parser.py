import requests
import time
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from utils.salary_processor import SalaryProcessor
from config import TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION


class SvoevagroParser:
    """
    Парсер вакансий с сайта svoevagro.ru (платформа «Я в Агро»).
    Использует schema.org микроразметку для надёжного извлечения данных.
    """

    def __init__(self):
        self.base_url = "https://svoevagro.ru"
        self.vacancies_url = f"{self.base_url}/vacancies"
        self.salary_processor = SalaryProcessor()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': self.base_url,
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
        })

    def search_vacancies(self, profession_name: str) -> List[Dict]:
        """
        Поиск вакансий по профессии.

        Args:
            profession_name: Поисковый термин из SEARCH_TERMS (напр. "тракторист")

        Returns:
            Список словарей с данными вакансий
        """
        vacancies = []

        try:
            search_url = f"{self.vacancies_url}?q={profession_name}"
            print(f"Запрос к svoevagro.ru: {search_url}")

            response = self.session.get(search_url, timeout=TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            vacancy_elements = self._find_vacancy_elements(soup)
            print(f"  Найдено элементов вакансий: {len(vacancy_elements)}")

            for element in vacancy_elements[:MAX_VACANCIES_PER_PROFESSION]:
                vacancy_data = self._parse_vacancy(element, profession_name)
                if vacancy_data:
                    vacancies.append(vacancy_data)

            # Пагинация
            page = 2
            while len(vacancies) < MAX_VACANCIES_PER_PROFESSION and page <= 5:
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

        except requests.exceptions.RequestException as e:
            print(f"Ошибка сети при парсинге svoevagro.ru: {e}")
        except Exception as e:
            print(f"Ошибка при парсинге svoevagro.ru: {e}")
            import traceback
            traceback.print_exc()

        return vacancies

    def _find_vacancy_elements(self, soup) -> List:
        """
        Поиск элементов вакансий на странице с использованием schema.org разметки.
        """
        # Основной метод: поиск по микроразметке JobPosting
        vacancy_elements = soup.find_all(itemtype="https://schema.org/JobPosting")

        # Фоллбэк: поиск по классу карточки
        if not vacancy_elements:
            vacancy_elements = soup.find_all(class_='vacancies-card')

        # Дополнительный фоллбэк: поиск по ссылке на вакансию
        if not vacancy_elements:
            vacancy_links = soup.find_all('a', href=re.compile(r'/vacancies/\d+'))
            for link in vacancy_links:
                parent = link.find_parent(itemtype="https://schema.org/JobPosting")
                if parent and parent not in vacancy_elements:
                    vacancy_elements.append(parent)
                else:
                    card = link.find_parent(class_='vacancies-card')
                    if card and card not in vacancy_elements:
                        vacancy_elements.append(card)

        return vacancy_elements[:50]

    def _parse_vacancy(self, element, search_term: str) -> Optional[Dict]:
        """
        Парсинг данных одной вакансии.

        Args:
            element: BeautifulSoup элемент карточки вакансии
            search_term: Поисковый термин (из SEARCH_TERMS)

        Returns:
            Словарь с данными вакансии или None при ошибке
        """
        try:
            # === ИНИЦИАЛИЗАЦИЯ ПЕРЕМЕННЫХ ПО УМОЛЧАНИЮ ===
            title = "Не указано"
            url = ""
            salary_text = ""
            currency = None
            salary_from = salary_to = average_salary = None
            city = "Не указан"
            company = ""
            employment = ""
            experience = ""
            date_posted = None

            # === ЗАГОЛОВОК И ССЫЛКА ===
            title_elem = element.find(itemprop="title") or element.find('h2', class_='name')
            if title_elem:
                title = title_elem.get_text(strip=True)
                link_elem = title_elem.find_parent('a', href=True)
                if link_elem:
                    url = link_elem.get('href', '')
                    if url and not url.startswith('http'):
                        url = self.base_url + url

            # === ЗАРПЛАТА (schema.org MonetaryAmount) ===
            salary_text = ""
            currency = None
            salary_block = element.find(itemprop="baseSalary", itemtype="https://schema.org/MonetaryAmount")

            if salary_block:
                # Валюта
                currency_meta = salary_block.find('meta', itemprop="currency")
                if currency_meta and currency_meta.get('content'):
                    currency = currency_meta['content']
                    if currency == 'RUR':
                        currency = 'RUB'

                # Числовое значение
                value_container = salary_block.find(
                    itemprop="value",
                    itemtype="https://schema.org/QuantitativeValue"
                )
                if value_container:
                    value_span = value_container.find('span', itemprop="value")
                    if value_span:
                        visible_text = value_span.get_text(strip=True)
                        if visible_text and re.search(r'\d{2,}', visible_text):
                            salary_text = visible_text
                        elif value_span.get('content'):
                            salary_text = value_span['content']
                    elif value_container.find('meta', itemprop="value"):
                        salary_text = value_container.find('meta', itemprop="value").get('content', '')
                    else:
                        salary_text = value_container.get_text(strip=True)

            # 🔑 НОРМАЛИЗАЦИЯ: убираем запятую как разделитель тысяч
            # '45,000 ₽' → '45000 ₽', '54,800 ₽' → '54800 ₽'
            if salary_text:
                salary_text = re.sub(r'(\d),(\d{3})', r'\1\2', salary_text)
                # Дополнительно: убираем лишние пробелы вокруг ₽
                salary_text = re.sub(r'\s*₽\s*', ' ₽', salary_text).strip()

            # 🔍 ОТЛАДКА
            print(f"  [SALARY_DEBUG] Raw: '{salary_text}' | Currency: {currency}")

            # Обработка через SalaryProcessor
            salary_from, salary_to, parsed_currency = self.salary_processor.parse_salary_text(salary_text)
            if parsed_currency:
                currency = parsed_currency
            average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            print(f"  [SALARY_DEBUG] Parsed: From={salary_from}, To={salary_to}, Avg={average_salary}, Curr={currency}")

            # === ГОРОД ===
            city_tooltip = element.find(class_='city-tooltip')
            if city_tooltip:
                city_value = city_tooltip.find('span', class_='value')
                if city_value:
                    city = city_value.get_text(strip=True)

            # Фоллбэк на meta
            if city == "Не указан":
                city_meta = element.find('meta', itemprop="addressLocality")
                if city_meta and city_meta.get('content'):
                    city = city_meta['content']
            city = self._clean_city_name(city)

            # === КОМПАНИЯ ===
            org_block = element.find(
                itemprop="hiringOrganization",
                itemtype="https://schema.org/Organization"
            )
            if org_block:
                company_elem = org_block.find(itemprop="name")
                if company_elem:
                    company = company_elem.get_text(strip=True)

            # === ТИП ЗАНЯТОСТИ ===
            employment_elem = element.find(itemprop="employmentType")
            if employment_elem:
                emp_type = employment_elem.get('content') or employment_elem.get_text(strip=True)
                employment_map = {
                    'FULL_TIME': 'Полная',
                    'PART_TIME': 'Неполная',
                    'CONTRACTOR': 'Проектная',
                    'TEMPORARY': 'Временная',
                    'INTERN': 'Стажировка'
                }
                employment = employment_map.get(emp_type.upper() if emp_type else '', emp_type)

            # === ОПЫТ РАБОТЫ ===
            exp_elem = element.find(itemprop="experienceRequirements")
            if exp_elem:
                experience = exp_elem.get_text(strip=True)
                if len(experience) > 200:
                    experience = experience[:197] + "..."

            # === ДАТА ПУБЛИКАЦИИ ===
            date_elem = element.find(itemprop="datePosted")
            if date_elem:
                date_posted = date_elem.get('content') or date_elem.get_text(strip=True)

            # === ВОЗВРАТ РЕЗУЛЬТАТА ===
            return {
                'profession_code': self._get_profession_code(title, search_term),
                'profession_name': search_term,
                'title': title,
                'city': city,
                'salary_from': salary_from,
                'salary_to': salary_to,
                'salary_average': average_salary,
                'currency': currency or 'RUB',
                'source': 'svoevagro.ru',
                'url': url,
                'company': company,
                'experience': experience,
                'employment': employment,
                'date_posted': date_posted
            }

        except Exception as e:
            print(f"Ошибка парсинга вакансии Svoevagro: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _clean_city_name(self, city: str) -> str:
        """Очистка и нормализация названия города"""
        if not city:
            return "Не указан"

        # Удаляем префиксы
        city = re.sub(r'^г\.?\s*', '', city, flags=re.I)
        city = re.sub(r'^город\s+', '', city, flags=re.I)

        # Удаляем содержимое в скобках
        city = re.sub(r'\([^)]*\)', '', city).strip()

        # Удаляем лишние пробелы
        city = re.sub(r'\s+', ' ', city).strip()

        # Удаляем запятые и точки в конце
        city = city.rstrip(',.').strip()

        return city if city else "Не указан"

    def _get_profession_code(self, title: str, search_term: str = None) -> str:
        """
        Определение кода профессии по заголовку вакансии.

        Логика приоритетов:
        1. Поиск целых ключевых слов из SEARCH_TERMS в заголовке вакансии
        2. Поиск полных названий профессий из PROFESSIONS в заголовке
        3. Фоллбэк: маппинг по поисковому термину (старая логика)

        Args:
            title: Заголовок вакансии с сайта (напр. "Тракторист-машинист категории В")
            search_term: Поисковый термин из SEARCH_TERMS (для обратной совместимости)

        Returns:
            Код профессии из справочника или "unknown"
        """
        from professions import PROFESSIONS

        if not title:
            title = ""

        title_lower = title.lower().strip()
        # Ключи должны точно совпадать с элементами SEARCH_TERMS
        term_to_code = {
            "механизация сельского хозяйства": "13.001",
            "птицевод": "13.002",
            "животновод": "13.003",
            "оператор машинного доения": "13.004",
            "агромелиорация": "13.005",
            "тракторист": "13.006",
            "фитосанитарный мониторинг": "13.008",
            "мастер растениеводства": "13.009",
            "животноводческий комплекс": "13.010",
            "обработчик шкур": "13.011",
            "ветеринария": "13.012",
            "зоотехния": "13.013",
            "пчеловод": "13.014",
            "декоративное садоводство": "13.015",
            "агроном": "13.017",
            "мелиоративные системы": "13.018",
            "селекционер животноводство": "13.020",
            "виноградарство": "13.021",
            "агрохимик": "13.023",
            "селекция генетика": "13.024",
            "семеноводство": "13.025",
        }

        # === ПРИОРИТЕТ 1: Поиск ключевых слов в заголовке вакансии ===
        # Сортируем по длине (убывание) для приоритета более специфичных фраз
        # Пример: "оператор машинного доения" приоритетнее, чем просто "оператор"
        sorted_terms = sorted(term_to_code.keys(), key=len, reverse=True)

        for term in sorted_terms:
            # Проверяем вхождение целого ключевого слова (регистронезависимо)
            if term.lower() in title_lower:
                return term_to_code[term]

        # === ПРИОРИТЕТ 2: Поиск по полным названиям из PROFESSIONS ===
        # На случай, если в заголовке используется официальное название профессии
        for code, full_name in PROFESSIONS.items():
            if full_name.lower() in title_lower:
                return code

        # === ПРИОРИТЕТ 3: Фоллбэк на старую логику (по search_term) ===
        # Для обратной совместимости, если заголовок пустой или не распознан
        if search_term:
            search_lower = search_term.lower().strip()
            # Прямое соответствие в term_to_code
            if search_lower in term_to_code:
                return term_to_code[search_lower]
            # Поиск по вхождению в названия PROFESSIONS
            for code, full_name in PROFESSIONS.items():
                if search_lower in full_name.lower():
                    return code

        return "unknown"