import re
import requests
from datetime import datetime


class SalaryProcessor:
    @staticmethod
    def parse_salary_hh(salary_data):
        if not salary_data:
            return None, None, None

        salary_from = salary_data.get('from')
        salary_to = salary_data.get('to')
        currency = salary_data.get('currency', 'RUB')

        # Конвертация в рубли
        if currency and currency != 'RUB':
            salary_from = SalaryProcessor.convert_currency(salary_from, currency)
            salary_to = SalaryProcessor.convert_currency(salary_to, currency)
            currency = 'RUB'

        return salary_from, salary_to, currency

    @staticmethod
    def parse_salary_superjob(salary_from, salary_to, currency):
        if not salary_from and not salary_to:
            return None, None, None
        if currency and currency != 'rub':
            salary_from = SalaryProcessor.convert_currency(salary_from, currency)
            salary_to = SalaryProcessor.convert_currency(salary_to, currency)
            currency = 'RUB'

        return salary_from, salary_to, currency

    @staticmethod
    def parse_salary_text(salary_text):
        if not salary_text or salary_text.strip() == '':
            return None, None, None

        numbers = re.findall(r'(\d[\d\s]*)\s*(?:руб|₽|р\.|RUB)', salary_text, re.IGNORECASE)
        if len(numbers) == 2:
            salary_from = int(re.sub(r'\s', '', numbers[0]))
            salary_to = int(re.sub(r'\s', '', numbers[1]))
            return salary_from, salary_to, 'RUB'
        elif len(numbers) == 1:
            salary = int(re.sub(r'\s', '', numbers[0]))
            # Определяем, "от" или "до"
            if 'от' in salary_text.lower():
                return salary, None, 'RUB'
            elif 'до' in salary_text.lower():
                return None, salary, 'RUB'
            else:
                return salary, salary, 'RUB'

        return None, None, None

    @staticmethod
    def convert_currency(amount, currency):
        if not amount:
            return None

        rates = {
            'USD': 82.13,
            'EUR': 95,
            'KZT': 0.1703,
            'BYN': 27.28,
            'UAH': 1.87
        }

        rate = rates.get(currency.upper(), 1)
        return int(amount * rate)

    @staticmethod
    def get_average_salary(salary_from, salary_to):
        if salary_from and salary_to:
            return (salary_from + salary_to) // 2
        elif salary_from:
            return salary_from
        elif salary_to:
            return salary_to
        return None