from config import HH_API_URL, TIMEOUT, DELAY, MAX_VACANCIES_PER_PROFESSION, MAX_WORKERS, MAX_CONNECTIONS, \
    MAX_CONNECTIONS_PER_HOST
import requests
import time
import threading
from typing import List, Dict, Optional
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

    # ID регионов
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

    # Поиск вакансий
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
                        if vacancy_data:  # Только релевантные вакансии
                            vacancies.append(vacancy_data)
                    if page >= data.get('pages', 0) - 1:
                        break
                    page += 1
                    time.sleep(0.3)
                except Exception as e:
                    break
        return vacancies

    # Парсинг вакансии
    def _parse_vacancy(self, vacancy: Dict, search_term: str) -> Optional[Dict]:
        # === ЗАГОЛОВОК ВАКАНСИИ ===
        title = vacancy.get('name', '')

        # === ПОЛУЧАЕМ КОД ПРОФЕССИИ ===
        # Передаем и заголовок, и поисковый запрос
        profession_code = self._get_profession_code(title, search_term)

        if profession_code == "unknown":
            print(f"  [FILTER] HH.ru: пропущена '{title[:60]}...' (искали: {search_term})")
            return None

        # === ЗАРПЛАТА ===
        salary_from, salary_to, currency = self.salary_processor.parse_salary_hh(
            vacancy.get('salary')
        )
        average_salary = self.salary_processor.get_average_salary(salary_from, salary_to)

        # === ДАТА ПУБЛИКАЦИИ ===
        date_posted = None
        published_at = vacancy.get('published_at')
        if published_at:
            date_posted = published_at[:10] if len(published_at) >= 10 else published_at

        return {
            'profession_code': profession_code,
            'profession_name': search_term,
            'title': title,
            'city': vacancy.get('area', {}).get('name', 'Не указан'),
            'salary_from': salary_from,
            'salary_to': salary_to,
            'salary_average': average_salary,
            'currency': currency or 'RUB',
            'source': 'hh.ru',
            'url': vacancy.get('alternate_url', ''),
            'company': vacancy.get('employer', {}).get('name', ''),
            'experience': vacancy.get('experience', {}).get('name', ''),
            'employment': vacancy.get('employment', {}).get('name', ''),
            'date_posted': date_posted
        }

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