#HeadHunter API настройки
HH_API_URL = "https://api.hh.ru/vacancies"
HH_AREAS_URL = "https://api.hh.ru/areas"

#SuperJob API настройки
SUPERJOB_API_URL = "https://api.superjob.ru/2.0/vacancies/"
SUPERJOB_API_TOWNS_URL = "https://api.superjob.ru/2.0/towns/"
SUPERJOB_API_KEY = "v3.r.139727585.4f9ea9fb1238e927037c0c93d5af0fdde7a62afe.dd5f69132c2bdbc05d8123ab1e162aa2fef2223b"

#Параметры парсинга
TIMEOUT = 10
DELAY = 0.3  # Уменьшена задержка для ускорения
MAX_VACANCIES_PER_PROFESSION = 100
MAX_VACANCIES_PER_CITY = 10
MAX_WORKERS = 6  # Увеличено количество потоков

# Настройки для управления соединениями
MAX_CONNECTIONS = 30  # Увеличено количество соединений
MAX_CONNECTIONS_PER_HOST = 8  # Увеличено соединений на хост

# Оптимизация HH.ru парсинга
HH_MAX_REGIONS = 50  # Ограничение на количество регионов для HH (вместо всех ~80)
HH_MAX_PAGES_PER_REGION = 5  # Ограничение страниц на регион (вместо 20)
HH_TOP_REGIONS_ONLY = True  # Использовать только топ регионы по количеству вакансий

# Настройки retry для запросов
MAX_RETRIES = 3
RETRY_BACKOFF = 0.5

#Режимы парсинга
PARSE_ALL_RUSSIA = True
AREAS_TO_PARSE = []

SUPERJOB_TOWNS = {}