import requests
import time
import re
import threading
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.salary_processor import SalaryProcessor
from config import (
    SUPERJOB_API_URL,
    SUPERJOB_API_TOWNS_URL,
    SUPERJOB_API_KEY,
    TIMEOUT,
    DELAY,
    MAX_VACANCIES_PER_PROFESSION,
    MAX_VACANCIES_PER_CITY,
    MAX_RETRIES,
    RETRY_BACKOFF,
    MAX_WORKERS,
    MAX_CONNECTIONS,
    MAX_CONNECTIONS_PER_HOST
)


class SuperJobParser:
    def __init__(self):
        self.base_url = "https://www.superjob.ru"
        self.api_url = SUPERJOB_API_URL
        self.api_towns_url = SUPERJOB_API_TOWNS_URL
        self.api_key = SUPERJOB_API_KEY
        self.salary_processor = SalaryProcessor()
        self.session = requests.Session()
        self.semaphore = threading.Semaphore(MAX_CONNECTIONS_PER_HOST)
        self.all_towns: Dict[str, int] = {}
        self._init_session()
        self.all_towns = self._get_all_towns()
        print(f"✅ Загружено {len(self.all_towns)} российских городов для парсинга SuperJob")

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
            pool_connections=MAX_CONNECTIONS,
            pool_maxsize=MAX_CONNECTIONS,
            max_retries=retry_strategy,
            pool_block=False
        )

        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        self.session.headers.update({
            'X-Api-App-Id': self.api_key,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
        })

    def search_vacancies(self, profession_name: str) -> List[Dict]:
        """
        Поиск вакансий по профессии по ВСЕЙ России через API SuperJob.
        Параллельный опрос городов с ограничением по количеству вакансий.
        """
        all_vacancies = []
        seen_urls = set()
        MAX_CITIES = 100  # 🔹 Ограничение на количество городов для одного поиска

        try:
            print(f"🔍 Поиск вакансий: '{profession_name}'")
            print(f"📊 Парсим до {MAX_VACANCIES_PER_PROFESSION} вакансий по {MAX_CITIES} городам...")

            # 🔹 Helper для безопасных запросов с ретраями
            def safe_get(url: str, params: dict = None, timeout=TIMEOUT, max_attempts=3):
                for attempt in range(max_attempts):
                    try:
                        with self.semaphore:
                            response = self.session.get(url, params=params, timeout=timeout)
                        response.raise_for_status()
                        return response
                    except requests.exceptions.Timeout:
                        if attempt < max_attempts - 1:
                            wait = DELAY * (2 ** attempt)
                            time.sleep(wait)
                        else:
                            return None
                    except requests.exceptions.RequestException as e:
                        if attempt < max_attempts - 1:
                            time.sleep(DELAY)
                        else:
                            print(f"  ❌ Ошибка запроса: {e}")
                            return None
                return None

            # 🔹 Подготовка списка городов
            towns_to_parse = list(self.all_towns.items())[:MAX_CITIES]
            print(f"🏙️  Будет обработано городов: {len(towns_to_parse)}")

            # 🔹 Параллельный запуск поиска по городам
            max_workers = min(MAX_WORKERS, 10)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        self._search_in_city,
                        profession_name,
                        town_id,
                        city_name,
                        safe_get
                    ): city_name
                    for city_name, town_id in towns_to_parse
                }

                for future in as_completed(futures):
                    city_name = futures[future]
                    try:
                        city_vacancies = future.result()
                        for vac in city_vacancies:
                            if vac['url'] not in seen_urls and len(all_vacancies) < MAX_VACANCIES_PER_PROFESSION:
                                all_vacancies.append(vac)
                                seen_urls.add(vac['url'])

                        if city_vacancies:
                            print(f"✅ {city_name}: +{len(city_vacancies)} вакансий (всего: {len(all_vacancies)})")

                        if len(all_vacancies) >= MAX_VACANCIES_PER_PROFESSION:
                            print(f"🎯 Достигнут лимит вакансий ({MAX_VACANCIES_PER_PROFESSION})")
                            break

                    except Exception as e:
                        print(f"❌ Ошибка в городе {city_name}: {e}")
                    time.sleep(DELAY * 0.5)  # 🔹 Небольшая пауза между городами

        except Exception as e:
            print(f"💥 Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()

        print(f"📦 Итого найдено: {len(all_vacancies)} вакансий по {len(seen_urls)} уникальным ссылкам")
        return all_vacancies

    def _search_in_city(self, profession_name: str, town_id: int, city_name: str, safe_get) -> List[Dict]:
        """
        Поиск вакансий в конкретном городе через API SuperJob
        """
        vacancies = []
        page = 0
        MAX_PAGES = 10
        consecutive_empty = 0

        try:
            while page < MAX_PAGES and len(vacancies) < MAX_VACANCIES_PER_CITY:
                params = {
                    'keyword': profession_name,
                    'town': town_id,
                    'count': 20,
                    'page': page
                }

                response = safe_get(self.api_url, params=params)
                if not response:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                    page += 1
                    continue

                data = response.json()
                items = data.get('objects', [])

                if not items:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                else:
                    consecutive_empty = 0
                    for item in items:
                        vacancy_data = self._parse_vacancy(item, profession_name, city_name)
                        if vacancy_data:
                            vacancies.append(vacancy_data)

                if not data.get('more'):
                    break

                page += 1
                time.sleep(DELAY * 0.3)

        except Exception as e:
            print(f"  ⚠️ Ошибка парсинга города {city_name}: {e}")

        return vacancies

    def _parse_vacancy(self, vacancy: Dict, search_term: str, city_name: str = None) -> Optional[Dict]:
        """Парсинг одной вакансии из API SuperJob"""
        try:
            # === ЗАРПЛАТА ===
            salary_from = vacancy.get('payment_from')
            salary_to = vacancy.get('payment_to')
            currency = vacancy.get('currency', 'RUB')

            salary_from, salary_to, currency = self.salary_processor.parse_salary_superjob(
                salary_from, salary_to, currency
            )
            average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            # === ЗАГОЛОВОК ===
            title = vacancy.get('profession', '').strip()
            if not title:
                return None

            # === URL ===
            url = vacancy.get('link', '')
            if not url:
                return None

            # === КОД ПРОФЕССИИ ===
            profession_code = self._get_profession_code(title, search_term)
            if profession_code == "unknown":
                return None  # 🔹 Фильтр нерелевантных вакансий

            # === ДАТА ПУБЛИКАЦИИ ===
            date_posted = self._parse_date(vacancy)

            # === ГОРОД ===
            city = city_name
            if not city:
                town = vacancy.get('town', {})
                city = town.get('title', 'Не указан') if isinstance(town, dict) else 'Не указан'
            city = self._clean_city(city)

            # === КОМПАНИЯ ===
            company = vacancy.get('firm_name', '')
            if isinstance(company, dict):
                company = company.get('name', '')

            # === ОПЫТ И ЗАНЯТОСТЬ ===
            experience = vacancy.get('experience', {})
            employment = vacancy.get('type_of_work', {})

            experience_text = experience.get('title', '') if isinstance(experience, dict) else str(experience)
            employment_text = employment.get('title', '') if isinstance(employment, dict) else str(employment)

            return {
                'profession_code': profession_code,
                'profession_name': search_term,
                'title': title,
                'city': city,
                'salary_from': salary_from,
                'salary_to': salary_to,
                'salary_average': average_salary,
                'currency': currency or 'RUB',
                'source': 'superjob.ru',
                'url': url,
                'company': company or 'Не указана',
                'experience': experience_text[:200] if experience_text else '',
                'employment': employment_text,
                'date_posted': date_posted
            }

        except Exception as e:
            print(f"  ⚠️ Ошибка парсинга вакансии: {e}")
            return None

    def _parse_date(self, vacancy: Dict) -> Optional[str]:
        """Парсинг даты публикации из разных форматов SuperJob API"""
        from datetime import datetime

        date_fields = ['date_published', 'publication_date', 'created']

        for field in date_fields:
            date_value = vacancy.get(field)
            if not date_value:
                continue

            try:
                # Unix timestamp (int/float)
                if isinstance(date_value, (int, float)):
                    date_obj = datetime.fromtimestamp(date_value)
                    return date_obj.strftime('%Y-%m-%d')

                # Строка с датой
                if isinstance(date_value, str):
                    date_value = date_value.strip()
                    if not date_value:
                        continue

                    # Формат "24.03.2026"
                    if '.' in date_value and len(date_value) == 10:
                        parts = date_value.split('.')
                        if len(parts) == 3:
                            return f"{parts[2]}-{parts[1]}-{parts[0]}"

                    # Формат "2026-03-24" или ISO
                    if '-' in date_value:
                        return date_value[:10]

                    # Попытка распарсить как timestamp в строке
                    try:
                        timestamp = int(date_value)
                        date_obj = datetime.fromtimestamp(timestamp)
                        return date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        pass

            except Exception:
                continue

        return None

    def _clean_city(self, city: str) -> str:
        """Очистка названия города (аналогично SvoevagroParser)"""
        if not city or city in ['Россия', 'РФ', '']:
            return "Не указан"

        prefixes = [
            r'^г\.?\s*', r'^город\s+', r'^п\.?\s*', r'^пос\.?\s*',
            r'^пгт\.?\s*', r'^с\.?\s*', r'^село\s+', r'^д\.?\s*',
            r'^дер\.?\s*', r'^ст\.?\s*', r'^ст-ца\.?\s*', r'^х\.?\s*',
            r'^хутор\s+', r'^сл\.?\s*', r'^клх\.?\s*', r'^рп\.?\s*',
            r'^г-к\.?\s*',
        ]

        for prefix in prefixes:
            city = re.sub(prefix, '', city, flags=re.I)

        city = re.sub(r'\([^)]*\)', '', city).strip()
        city = re.sub(r'\s+', ' ', city).strip()
        city = city.rstrip(',.').strip()

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
            'Нижневартовск', 'Новороссийск', 'Йошкар-Ола', 'Таганрог',
        }

        if city in known_cities:
            return city

        for known in known_cities:
            if known.lower() in city.lower():
                return known

        if len(city) >= 2 and city[0].isupper():
            return city

        return "Не указан"

    def _is_russian_city(self, city_name: str) -> bool:
        """Фильтрация нероссийских городов"""
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
        if any('\u0400' <= char <= '\u04FF' for char in city_name):
            return True
        return False

    def _get_default_towns(self) -> Dict[str, int]:
        """Fallback-список городов при ошибке API"""
        return {
            "Москва": 4, "Санкт-Петербург": 5, "Екатеринбург": 66,
            "Новосибирск": 70, "Казань": 88, "Нижний Новгород": 61,
            "Самара": 78, "Омск": 69, "Челябинск": 92, "Ростов-на-Дону": 76,
            "Уфа": 85, "Красноярск": 58, "Пермь": 72, "Воронеж": 30,
            "Волгоград": 29, "Краснодар": 56, "Саратов": 79, "Тюмень": 83,
        }

    def _get_all_towns(self) -> Dict[str, int]:
        """Получение списка городов через API SuperJob"""
        all_towns = {}
        page = 0

        try:
            while True:
                params = {'page': page, 'count': 100}
                response = self.session.get(self.api_towns_url, params=params, timeout=TIMEOUT)
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
                time.sleep(DELAY * 0.2)

        except Exception as e:
            print(f"⚠️ Ошибка загрузки городов: {e}")
            return self._get_default_towns()

        # 🔹 Фильтрация только российских городов
        russian_towns = {
            name: tid for name, tid in all_towns.items()
            if self._is_russian_city(name)
        }
        return russian_towns

    def _get_profession_code(self, title: str, search_term: str = None) -> str:
        """
        Определение кода профессии по заголовку вакансии и поисковому запросу.
        Возвращает код ТОЛЬКО если вакансия строго соответствует профессии.
        """
        if not title:
            return "unknown"

        title_lower = title.lower().strip()
        search_term_lower = search_term.lower().strip() if search_term else ""

        keywords_map = {
            "13.001": ["механизатор", "механизация сельского", "агротехник", "механизатор сельского",
                       "техник-механизатор"],
            "13.002": ["птицевод", "оператор птицеводства", "птицефабрика", "птичник", "птичница", "птицеводство"],
            "13.003": ["животновод", "скотовод", "животноводство", "крс", "мрс", "оператор животноводства",
                       "фермер животновод", "гуртоправ"],
            "13.004": ["оператор машинного доения", "дояр", "доярка", "машинное доение", "оператор доения"],
            "13.005": ["агромелиорация", "мелиоратив", "осушение", "орошение", "мелиоратор", "агромелиоратор"],
            "13.006": ["тракторист", "тракторист-машинист", "машинист трактора", "водитель трактора", "трактор",
                       "механизатор трактор"],
            "13.008": ["фитосанитарный", "фитосанитар", "карантин растений", "защита растений", "фитопатолог"],
            "13.009": ["мастер растениеводства", "растениевод", "агроном растениевод", "специалист по растениеводству",
                       "растениеводство"],
            "13.010": ["оператор животноводческих комплексов", "животноводческий комплекс", "оператор фермы",
                       "механизированная ферма", "оператор мтф"],
            "13.011": ["обработчик шкур", "скорняк", "обработка шкур", "кожевник"],
            "13.012": ["ветеринар", "ветеринария", "ветврач", "ветеринарный врач", "ветфельдшер",
                       "ветеринарный фельдшер", "ветеринар-хирург"],
            "13.013": ["зоотехник", "зоотехния", "специалист по зоотехнии", "зооинженер"],
            "13.014": ["пчеловод", "пасечник", "пчеловодство", "бортник"],
            "13.015": ["декоративное садоводство", "садовод декоративный", "ландшафтный садовод", "флорист-садовод"],
            "13.017": ["агроном", "агрономия", "агроном-растениевод", "главный агроном"],
            "13.018": ["мелиоративные системы", "эксплуатация мелиоративных", "мелиоратор"],
            "13.020": ["селекционер животноводство", "селекция животноводство", "племенное животноводство",
                       "селекционер-животновод"],
            "13.021": ["виноградарь", "виноградарство", "винодел", "виноградарь-винодел"],
            "13.023": ["агрохимик", "агрохимия", "почвовед", "агрохимик-почвовед"],
            "13.024": ["селекция генетика животноводство", "генетика животноводство", "селекционер-генетик",
                       "селекция животных"],
            "13.025": ["семеноводство", "семеновод", "селекция растениеводство", "специалист по семеноводству"],
        }

        def is_relevant(text_lower: str, keywords: List[str]) -> bool:
            return any(kw.lower() in text_lower for kw in keywords)

        # 🔹 Шаг 1: Определяем код по поисковому запросу
        search_term_code = None
        for code, keywords in keywords_map.items():
            if is_relevant(search_term_lower, keywords):
                search_term_code = code
                break

        if search_term_code:
            # 🔹 Шаг 2: Проверяем, что заголовок соответствует той же профессии
            if is_relevant(title_lower, keywords_map.get(search_term_code, [])):
                return search_term_code
            else:
                return "unknown"  # 🔹 Строгий фильтр

        # 🔹 Шаг 3: Если запрос не распознан, проверяем заголовок напрямую
        if search_term_lower and search_term_lower in title_lower:
            for code, keywords in keywords_map.items():
                if is_relevant(title_lower, keywords):
                    return code

        return "unknown"