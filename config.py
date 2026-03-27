#HeadHunter API настройки
HH_API_URL = "https://api.hh.ru/vacancies"
HH_AREAS_URL = "https://api.hh.ru/areas"

#SuperJob API настройки
SUPERJOB_API_URL = "https://api.superjob.ru/2.0/vacancies/"
SUPERJOB_API_TOWNS_URL = "https://api.superjob.ru/2.0/towns/"
SUPERJOB_API_KEY = "v3.r.139727585.4f9ea9fb1238e927037c0c93d5af0fdde7a62afe.dd5f69132c2bdbc05d8123ab1e162aa2fef2223b"

#Параметры парсинга
TIMEOUT = 10
DELAY = 0.5
MAX_VACANCIES_PER_PROFESSION = 100
MAX_VACANCIES_PER_CITY = 10
MAX_WORKERS = 4

#Настройки для управления соединениями
MAX_CONNECTIONS = 20  #Максимальное количество соединений
MAX_CONNECTIONS_PER_HOST = 5  #Максимальное соединений на хост

#Режимы парсинга
PARSE_ALL_RUSSIA = True
AREAS_TO_PARSE = []

SUPERJOB_TOWNS = {}