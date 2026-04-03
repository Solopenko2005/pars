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
        # Базовый список городов на случай ошибки API
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

    # Получение всех городов через API SuperJob
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

    # Поиск вакансий на SuperJob по всем городам
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

    # Поиск вакансий в конкретном городе
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
                        if vacancy_data:  # Только релевантные вакансии
                            vacancies.append(vacancy_data)
                    if not data.get('more'):
                        break
                    page += 1
                    time.sleep(0.3)
                except Exception as e:
                    break
        return vacancies

    # Парсинг одной вакансии
    def _parse_vacancy(self, vacancy: Dict, search_term: str, city_name: str = None) -> Optional[Dict]:
        try:
            # === ЗАРПЛАТА ===
            salary_from = vacancy.get('payment_from')
            salary_to = vacancy.get('payment_to')
            currency = vacancy.get('currency')
            salary_from, salary_to, currency = self.salary_processor.parse_salary_superjob(
                salary_from, salary_to, currency
            )
            average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

            # === ЗАГОЛОВОК ВАКАНСИИ ===
            title = vacancy.get('profession', '')

            # === ПОЛУЧАЕМ КОД ПРОФЕССИИ ===
            # Передаем и заголовок, и поисковый запрос
            profession_code = self._get_profession_code(title, search_term)

            if profession_code == "unknown":
                print(f"  [FILTER] SuperJob: пропущена '{title[:60]}...' (искали: {search_term})")
                return None

            date_posted = None

            # SuperJob API использует поле 'date_published' (Unix timestamp в секундах)
            # Пример: "date_published": 1742800000
            date_published = vacancy.get('date_published')

            if date_published:
                try:
                    # Если это Unix timestamp (число)
                    if isinstance(date_published, (int, float)):
                        from datetime import datetime
                        date_obj = datetime.fromtimestamp(date_published)
                        date_posted = date_obj.strftime('%Y-%m-%d')
                    # Если это строка с датой
                    elif isinstance(date_published, str):
                        if '.' in date_published:  # Формат "24.03.2026"
                            parts = date_published.split('.')
                            if len(parts) == 3:
                                date_posted = f"{parts[2]}-{parts[1]}-{parts[0]}"
                        elif '-' in date_published:  # Формат "2026-03-24"
                            date_posted = date_published[:10]
                        else:
                            # Возможно, это тоже timestamp в виде строки
                            try:
                                timestamp = int(date_published)
                                from datetime import datetime
                                date_obj = datetime.fromtimestamp(timestamp)
                                date_posted = date_obj.strftime('%Y-%m-%d')
                            except ValueError:
                                date_posted = date_published[:10] if len(date_published) >= 10 else date_published
                except Exception as e:
                    print(f"  [WARN] Ошибка парсинга даты SuperJob: {e}, значение: {date_published}")
                    date_posted = None

            # Если date_published не найден, пробуем другие возможные поля
            if not date_posted:
                # Пробуем publication_date
                pub_date = vacancy.get('publication_date')
                if pub_date:
                    if isinstance(pub_date, str):
                        if '.' in pub_date:
                            parts = pub_date.split('.')
                            if len(parts) == 3:
                                date_posted = f"{parts[2]}-{parts[1]}-{parts[0]}"
                        elif '-' in pub_date:
                            date_posted = pub_date[:10]
                        else:
                            date_posted = pub_date[:10] if len(pub_date) >= 10 else pub_date

            city = city_name or vacancy.get('town', {}).get('title', 'Не указан')

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
                'url': vacancy.get('link', ''),
                'company': vacancy.get('firm_name', ''),
                'experience': vacancy.get('experience', {}).get('title', ''),
                'employment': vacancy.get('type_of_work', {}).get('title', ''),
                'date_posted': date_posted
            }
        except Exception as e:
            print(f"Ошибка парсинга вакансии SuperJob: {e}")
            return None

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
            "13.001": [  # Механизация сельского хозяйства
                "механизатор", "механизация сельского", "агротехник",
                "механизатор сельского", "техник-механизатор"
            ],
            "13.002": [  # Птицевод
                "птицевод", "оператор птицеводства", "птицефабрика",
                "птичник", "птичница", "птицеводство"
            ],
            "13.003": [  # Животновод
                "животновод", "скотовод", "животноводство", "крс", "мрс",
                "оператор животноводства", "фермер животновод", "гуртоправ"
            ],
            "13.004": [  # Оператор машинного доения
                "оператор машинного доения", "дояр", "доярка",
                "машинное доение", "оператор доения"
            ],
            "13.005": [  # Агромелиорация
                "агромелиорация", "мелиоратив", "осушение", "орошение",
                "мелиоратор", "агромелиоратор"
            ],
            "13.006": [  # Тракторист
                "тракторист", "тракторист-машинист", "машинист трактора",
                "водитель трактора", "трактор", "механизатор трактор"
            ],
            "13.008": [  # Фитосанитарный мониторинг
                "фитосанитарный", "фитосанитар", "карантин растений",
                "защита растений", "фитопатолог"
            ],
            "13.009": [  # Мастер растениеводства
                "мастер растениеводства", "растениевод", "агроном растениевод",
                "специалист по растениеводству", "растениеводство"
            ],
            "13.010": [  # Оператор животноводческих комплексов
                "оператор животноводческих комплексов", "животноводческий комплекс",
                "оператор фермы", "механизированная ферма", "оператор мтф"
            ],
            "13.011": [  # Обработчик шкур
                "обработчик шкур", "скорняк", "обработка шкур", "кожевник"
            ],
            "13.012": [  # Ветеринария
                "ветеринар", "ветеринария", "ветврач", "ветеринарный врач",
                "ветфельдшер", "ветеринарный фельдшер", "ветеринар-хирург"
            ],
            "13.013": [  # Зоотехния
                "зоотехник", "зоотехния", "специалист по зоотехнии", "зооинженер"
            ],
            "13.014": [  # Пчеловод
                "пчеловод", "пасечник", "пчеловодство", "бортник"
            ],
            "13.015": [  # Декоративное садоводство
                "декоративное садоводство", "садовод декоративный",
                "ландшафтный садовод", "флорист-садовод"
            ],
            "13.017": [  # Агроном
                "агроном", "агрономия", "агроном-растениевод", "главный агроном"
            ],
            "13.018": [  # Мелиоративные системы
                "мелиоративные системы", "эксплуатация мелиоративных", "мелиоратор"
            ],
            "13.020": [  # Селекционер животноводство
                "селекционер животноводство", "селекция животноводство",
                "племенное животноводство", "селекционер-животновод"
            ],
            "13.021": [  # Виноградарство
                "виноградарь", "виноградарство", "винодел", "виноградарь-винодел"
            ],
            "13.023": [  # Агрохимик
                "агрохимик", "агрохимия", "почвовед", "агрохимик-почвовед"
            ],
            "13.024": [  # Селекция генетика животноводство
                "селекция генетика животноводство", "генетика животноводство",
                "селекционер-генетик", "селекция животных"
            ],
            "13.025": [  # Семеноводство
                "семеноводство", "семеновод", "селекция растениеводство",
                "специалист по семеноводству"
            ],
        }
        def is_relevant(text_lower: str, keywords: List[str]) -> bool:
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return True
            return False

        search_term_code = None
        for code, keywords in keywords_map.items():
            if is_relevant(search_term_lower, keywords):
                search_term_code = code
                break

        if search_term_code:
            # Проверяем, что заголовок вакансии также соответствует этой профессии
            if is_relevant(title_lower, keywords_map.get(search_term_code, [])):
                return search_term_code
            else:
                # Заголовок не соответствует - вакансия нерелевантна
                return "unknown"

        # Проверяем соответствие заголовка поисковому запросу напрямую
        if search_term_lower and search_term_lower in title_lower:
            # Ищем код по ключевым словам в заголовке
            for code, keywords in keywords_map.items():
                if is_relevant(title_lower, keywords):
                    return code

        return "unknown"