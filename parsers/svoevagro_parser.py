import requests
import time
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from utils.salary_processor import SalaryProcessor
from config import TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION, MAX_RETRIES, RETRY_BACKOFF
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class SvoevagroParser:
    def __init__(self):
        self.base_url = "https://svoevagro.ru"
        self.vacancies_url = f"{self.base_url}/vacancies"
        self.salary_processor = SalaryProcessor()
        self.session = requests.Session()
        self._init_session()

    def _init_session(self):
        """
        Настройка сессии с повторными попытками и пулом соединений
        """
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=retry_strategy,
            pool_block=False
        )

        self.session = requests.Session()
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': self.base_url,
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'max-age=0',  # 🔹 Избегаем кэша
        })

    def search_vacancies(self, profession_name: str, region: str = 'russia') -> List[Dict]:
        """
        Поиск вакансий по профессии по ВСЕЙ России с неограниченной пагинацией.
        """
        vacancies = []
        seen_urls = set()
        MAX_PAGES = 500

        try:
            search_url = f"{self.vacancies_url}?search={requests.utils.quote(profession_name)}&region={region}"
            print(f"Запрос: {search_url}")
            print(f"Парсим страницы до {MAX_VACANCIES_PER_PROFESSION} вакансий или конца результатов...")

            def safe_get(url, timeout=TIMEOUT, max_attempts=3):
                for attempt in range(max_attempts):
                    try:
                        response = self.session.get(url, timeout=timeout)
                        response.raise_for_status()
                        return response
                    except requests.exceptions.Timeout:
                        if attempt < max_attempts - 1:
                            wait = DELAY * (2 ** attempt)
                            time.sleep(wait)
                        else:
                            return None
                    except requests.exceptions.RequestException:
                        if attempt < max_attempts - 1:
                            time.sleep(DELAY)
                        else:
                            return None
                return None

            page = 1
            consecutive_empty = 0

            while len(vacancies) < MAX_VACANCIES_PER_PROFESSION and page <= MAX_PAGES:

                if page == 1:
                    url = search_url
                else:
                    url = f"{search_url}&page={page}"

                response = safe_get(url)
                if not response:
                    print(f"Страница {page}: ошибка загрузки, пропускаем")
                    page += 1
                    consecutive_empty += 1
                    if consecutive_empty >= 3:  # 🔹 3 ошибки подряд = конец
                        print("3 ошибки подряд — завершаем парсинг")
                        break
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                new_elements = self._find_vacancy_elements(soup)

                if not new_elements:
                    consecutive_empty += 1
                    print(f"Страница {page}: нет вакансий (пустых подряд: {consecutive_empty})")

                    if consecutive_empty >= 2:
                        print("Достигнут конец результатов")
                        break
                    page += 1
                    time.sleep(DELAY)
                    continue

                consecutive_empty = 0
                print(f"Страница {page}: +{len(new_elements)} элементов (всего вакансий: {len(vacancies)})")

                for element in new_elements:
                    if len(vacancies) >= MAX_VACANCIES_PER_PROFESSION:
                        break

                    vacancy_data = self._parse_vacancy(element, profession_name)
                    if vacancy_data and vacancy_data['url'] not in seen_urls:
                        vacancies.append(vacancy_data)
                        seen_urls.add(vacancy_data['url'])

                page += 1
                time.sleep(DELAY)

            if page > MAX_PAGES:
                print(f"Достигнут лимит страниц ({MAX_PAGES})")
            elif consecutive_empty >= 2:
                print(f"Все страницы обработаны")

        except Exception as e:
            print(f"Ошибка: {e}")
            import traceback
            traceback.print_exc()

        print(f"Итого найдено: {len(vacancies)} вакансий за {page - 1} страниц")
        return vacancies

    def _find_vacancy_elements(self, soup) -> List:
        """
        Поиск элементов вакансий по schema.org разметке
        """
        elements = soup.find_all(itemtype="https://schema.org/JobPosting")
        if not elements:
            elements = soup.find_all(class_='vacancies-card')

        if not elements:
            links = soup.find_all('a', href=re.compile(r'/vacancies/\d+'))
            for link in links:
                parent = link.find_parent(itemtype="https://schema.org/JobPosting")
                if parent and parent not in elements:
                    elements.append(parent)

        return elements[:50]

    def _parse_vacancy(self, element, search_term: str) -> Optional[Dict]:
        """Парсинг одной вакансии"""
        try:
            title = "Не указано"
            url = ""

            title_elem = element.find(itemprop="title") or element.find('h2', class_='name') or element.find('h3')
            if title_elem:
                title = title_elem.get_text(strip=True)
                link = title_elem.find_parent('a', href=True)
                if link and link.get('href'):
                    url = link['href']
                    if not url.startswith('http'):
                        url = self.base_url + url

            if not url:
                return None

            salary_from = salary_to = average_salary = None
            currency = 'RUB'

            salary_block = element.find(itemprop="baseSalary", itemtype="https://schema.org/MonetaryAmount")
            if salary_block:
                # Валюта
                curr_meta = salary_block.find('meta', itemprop="currency")
                if curr_meta and curr_meta.get('content'):
                    currency = 'RUB' if curr_meta['content'] == 'RUR' else curr_meta['content']

                # Значение
                value_cont = salary_block.find(itemprop="value", itemtype="https://schema.org/QuantitativeValue")
                if value_cont:
                    val_span = value_cont.find('span', itemprop="value")
                    if val_span:
                        salary_text = val_span.get_text(strip=True) or val_span.get('content', '')
                    else:
                        salary_text = value_cont.find('meta', itemprop="value")
                        salary_text = salary_text.get('content', '') if salary_text else value_cont.get_text(strip=True)

                    if salary_text:
                        salary_text = re.sub(r'(\d),(\d{3})', r'\1\2', salary_text)
                        salary_text = re.sub(r'\s*₽\s*', ' ₽', salary_text).strip()

                        salary_from, salary_to, parsed_curr = self.salary_processor.parse_salary_text(salary_text)
                        if parsed_curr:
                            currency = parsed_curr
                        average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            city = "Не указан"

            info_block = element.find('div', class_='information')
            if info_block:
                label = info_block.find('span', class_='label')
                if label and label.get_text(strip=True) == 'Город':
                    city_value = info_block.find('span', class_='value')
                    if city_value:
                        city = city_value.get_text(strip=True)

            if city == "Не указан":
                tooltip = element.find(class_='city-tooltip')
                if tooltip:
                    val = tooltip.find('span', class_='value')
                    if val:
                        city = val.get_text(strip=True)

                if city == "Не указан":
                    addr = element.find('a', class_='company-address')
                    if addr:
                        val = addr.find('span', class_='value')
                        if val:
                            city = val.get_text(strip=True)

            if city == "Не указан":
                meta = element.find('meta', itemprop="addressLocality")
                if meta and meta.get('content'):
                    city = meta['content']

            city = self._clean_city(city)

            company = ""
            org = element.find(itemprop="hiringOrganization", itemtype="https://schema.org/Organization")
            if org:
                name = org.find(itemprop="name")
                if name:
                    company = name.get_text(strip=True)

            employment = ""
            emp = element.find(itemprop="employmentType")
            if emp:
                emp_val = emp.get('content') or emp.get_text(strip=True)
                emp_map = {
                    'FULL_TIME': 'Полная', 'PART_TIME': 'Неполная',
                    'CONTRACTOR': 'Проектная', 'TEMPORARY': 'Временная', 'INTERN': 'Стажировка'
                }
                employment = emp_map.get(emp_val.upper() if emp_val else '', emp_val)

            experience = ""
            exp = element.find(itemprop="experienceRequirements")
            if exp:
                experience = exp.get_text(strip=True)[:200]

            date_posted = None
            date = element.find(itemprop="datePosted")
            if date:
                date_posted = date.get('content') or date.get_text(strip=True)
                if date_posted and len(date_posted) > 10:
                    date_posted = date_posted[:10]

            profession_code = self._get_profession_code(title, search_term)
            if profession_code == "unknown":
                return None

            return {
                'profession_code': profession_code,
                'profession_name': search_term,
                'title': title,
                'city': city,
                'salary_from': salary_from,
                'salary_to': salary_to,
                'salary_average': average_salary,
                'currency': currency,
                'source': 'svoevagro.ru',
                'url': url,
                'company': company,
                'experience': experience,
                'employment': employment,
                'date_posted': date_posted
            }

        except Exception as e:
            print(f"Ошибка парсинга: {e}")
            return None

    def _clean_city(self, city: str) -> str:
        """Очистка названия города/населённого пункта"""
        if not city or city in ['Россия', 'РФ', '']:
            return "Не указан"

        prefixes = [
            r'^г\.?\s*',  # г., г
            r'^город\s+',  # город
            r'^п\.?\s*',  # п., п (посёлок)
            r'^пос\.?\s*',  # пос., пос (посёлок)
            r'^пгт\.?\s*',  # пгт (посёлок городского типа)
            r'^с\.?\s*',  # с., с (село)
            r'^село\s+',  # село
            r'^д\.?\s*',  # д., д (деревня)
            r'^дер\.?\s*',  # дер. (деревня)
            r'^ст\.?\s*',  # ст., ст (станица)
            r'^ст-ца\.?\s*',  # ст-ца (станица)
            r'^х\.?\s*',  # х., х (хутор)
            r'^хутор\s+',  # хутор
            r'^сл\.?\s*',  # сл. (слобода)
            r'^клх\.?\s*',  # клх (колхоз)
            r'^рп\.?\s*',  # рп (рабочий посёлок)
            r'^г-к\.?\s*',  # г-к (городок)
        ]

        for prefix in prefixes:
            city = re.sub(prefix, '', city, flags=re.I)

        # Удаляем лишнее
        city = re.sub(r'\([^)]*\)', '', city).strip()  # скобки
        city = re.sub(r'\s+', ' ', city).strip()  # лишние пробелы
        city = city.rstrip(',.').strip()  # знаки в конце

        known_cities = {
            'Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Казань',
            'Нижний Новгород', 'Челябинск', 'Самара', 'Омск', 'Ростов-на-Дону',
            'Уфа', 'Красноярск', 'Пермь', 'Воронеж', 'Волгоград', 'Краснодар',
            'Саратов', 'Тюмень', 'Тольятти', 'Ижевск', 'Барнаул', 'Ульяновск',
            'Иркутск', 'Хабаровск', 'Ярославль', 'Владивосток', 'Махачкала', 'Томск',
            'Оренбург', 'Кемерово', 'Новокузнецк', 'Рязань', 'Астрахань', 'Пенза',
            'Липецк', 'Тула', 'Киров', 'Чебоксары', 'Калининград', 'Брянск',
            'Курск', 'Иваново', 'Магнитогорск', 'Тверь', 'Ставрополь', 'Белгород',
            'Сочи', 'Нижний Тагил', 'Архангельск', 'Владимир', 'Чита', 'Сургут',
            'Калуга', 'Смоленск', 'Курган', 'Орёл', 'Череповец', 'Владикавказ',
            'Мурманск', 'Тамбов', 'Грозный', 'Стерлитамак', 'Кострома', 'Петрозаводск',
            'Нижневартовск', 'Новороссийск', 'Йошкар-Ола', 'Таганрог', 'Волово',
        }

        if city in known_cities:
            return city

        for known in known_cities:
            if known.lower() in city.lower():
                return known

        if len(city) >= 2 and city[0].isupper():
            return city

        return "Не указан"

    def _get_profession_code(self, title: str, search_term: str) -> str:
        """Определение кода профессии"""
        if not title:
            return "unknown"

        title_lower = title.lower()
        search_lower = search_term.lower()

        keywords_map = {
            "13.001": ["механизатор", "механизация сельского", "агротехник", "техник-механизатор"],
            "13.002": ["птицевод", "птицефабрика", "птичник", "птичница", "птицеводство"],
            "13.003": ["животновод", "скотовод", "животноводство", "оператор животноводства", "крс", "мрс"],
            "13.004": ["оператор машинного доения", "дояр", "доярка", "машинное доение"],
            "13.005": ["агромелиорация", "мелиоратор", "орошение", "осушение"],
            "13.006": ["тракторист", "тракторист-машинист", "машинист трактора", "водитель трактора"],
            "13.008": ["фитосанитар", "защита растений", "фитопатолог", "карантин растений"],
            "13.009": ["растениевод", "мастер растениеводства", "растениеводство"],
            "13.010": ["оператор животноводческих комплексов", "оператор фермы", "мтф"],
            "13.011": ["обработчик шкур", "скорняк", "кожевник"],
            "13.012": ["ветеринар", "ветеринария", "ветврач", "ветеринарный врач", "ветфельдшер"],
            "13.013": ["зоотехник", "зоотехния", "зооинженер", "специалист по зоотехнии"],
            "13.014": ["пчеловод", "пасечник", "пчеловодство", "бортник"],
            "13.015": ["декоративное садоводство", "ландшафтный садовод"],
            "13.017": ["агроном", "агрономия", "агроном-растениевод", "главный агроном"],
            "13.018": ["мелиоративные системы", "эксплуатация мелиоративных"],
            "13.020": ["селекционер животноводство", "племенное животноводство"],
            "13.021": ["виноградарь", "виноградарство", "винодел"],
            "13.023": ["агрохимик", "агрохимия", "почвовед"],
            "13.024": ["селекция генетика животноводство", "селекционер-генетик"],
            "13.025": ["семеноводство", "семеновод", "специалист по семеноводству"],
        }

        target_code = None
        for code, keywords in keywords_map.items():
            if any(kw in search_lower for kw in keywords):
                target_code = code
                break

        if target_code:
            if any(kw in title_lower for kw in keywords_map[target_code]):
                return target_code
            return "unknown"

        if search_lower in title_lower:
            for code, keywords in keywords_map.items():
                if any(kw in title_lower for kw in keywords):
                    return code
            return f"search:{search_lower.replace(' ', '_')}"

        return "unknown"