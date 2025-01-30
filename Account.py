from utils import random_string
from string import ascii_uppercase, ascii_lowercase, digits
from random import choice, random
from datetime import datetime, timedelta

account_types = [
    'BNESS TRACKER',
    'BNESS SAVINGS',
    'BNESS CHEQUE',
    'BNESS MMA',
    'BNESS CD',
    'MERCHANT',
    'FOREIGN CURRENCY',
    'MULTI CURRENCY',
    'CORP CURRENT'
]

transaction_types = [
    "TRANSFER",
    "DEPOSIT",
    "WITHDRAWAL",
    "PAYMENT",
    "CHEQUE",
    "INTEREST",
    "FEE",
    "FX"
]


def choose_currencies(account_type):
    base_currency = ['GBP'] if random() < 0.67 else ['USD']
    secondary_currency = choice(['EUR', 'JPY'])

    if account_type == 'FOREIGN CURRENCY':
        return [secondary_currency]
    elif account_type == 'MULTI CURRENCY':
        return [base_currency, secondary_currency]
    else:
        return base_currency


# def choose_transactions_per_day(account_type):
#     if account_type not in ['BNESS SAVINGS', 'FOREIGN CURRENCY', 'MULTI CURRENCY']:
#         return 10000 if random() < 0.1 else 5000 if random() < 0.3 else 1000
#     else:
#         return 50 if random() < 0.1 else 10 if random() < 0.3 else 1


def random_date_in_range(start, end):
    return start + timedelta(seconds=random() * (end - start).total_seconds())


class Account:
    def __init__(self, days):
        self.name = random_string(3, ascii_uppercase) + "ACCNAME"
        self.number = random_string(12, digits)
        self.type = choice(account_types),
        self.currencies = choose_currencies(self.type)
        self.days = days


    def create_transaction(self):
        random_datetime = random_date_in_range(datetime.now() - timedelta(days=self.days), datetime.now())
        random_date = datetime(random_datetime.year, random_datetime.month, random_datetime.day)
        return {
            "AccEntrStsFlg": random_string(10),
            "Acc": {
                "Nm": self.name,
                "No": self.number,
                "Tp": self.type,
            },
            "Amt": random() * 99999,
            "BnkId": random_string(10),
            "BnkRf": "BankRef" + random_string(3, digits),
            "Ccy": choice(self.currencies),
            "ChqNo": "CheqNum" + random_string(16, digits),
            "ChqPmtInfId": random_string(10),
            "Cld": {
                "FteDt": random_datetime,
                "OrIntDt": random_datetime,
                "Sts": random_datetime
            },
            "CtrPty": {
                "AccNo": random_string(12, digits),
                "SrtCd": random_string(10),
                "Nm": random_string(3, ascii_uppercase) + "ACCNM"
            },
            "CstRf": "CustRef" + random_string(27, digits + ascii_lowercase),
            "DCI": choice(['C', 'D']),
            "EntCrtdDtTm": random_datetime,
            "EntrDt": random_date,
            "Ibn": random_string(2, ascii_uppercase) + "IBN",
            "InpDt": random_datetime,
            "InpTm": random_string(10),
            "Nrr": {
                "1": random_string(10),
                "2": random_string(10),
                "L1": random_string(10),
                "L2": random_string(10),
                "L3": random_string(10),
                "L4": random_string(10)
            },
            "NoTx": random() * 99999,
            "OfsAccHTg": random_string(10),
            "PtyTp": random_string(10),
            "Rsn": random_string(10),
            "ScIdr": choice(['BCBS', 'USCB', 'UKBA']),
            "ScSysTrnRf": random_string(10),
            "TrnCd9": random_string(10),
            "TxDts": random_string(10),
            "TxEntDt": random_date,
            "TxTp": choice(transaction_types),
            "VlDt": random_datetime
        }
