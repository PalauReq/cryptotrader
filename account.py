from datetime import datetime, timezone

# TODO: add protections against negative balances


# TODO: move to utils
def now():
    return datetime.now(timezone.utc).isoformat()


class Account:
    FEE = 0
    LEDGER_COLUMN_NAMES = ["ledger_id", "operation_type", "currency_symbol", "amount", "fee", "timestamp", "balance"]

    def __init__(self):
        self.ledger_id = 0
        self.balances = {}
        self.ledger = []

    def deposit(self, currency_symbol, amount, fee=FEE, timestamp=now()):
        self.update_balance(currency_symbol, amount)
        self.append_ledger("deposit", currency_symbol, amount, fee, timestamp, self.get_balance(currency_symbol))

    def withdraw(self, currency_symbol, amount, fee=FEE, timestamp=now()):
        self.update_balance(currency_symbol, -1 * amount)
        self.append_ledger("withdrawal", currency_symbol, -1 * amount, fee, timestamp,
                           self.get_balance(currency_symbol))

    def trade(self, buy_currency_symbol, buy_amount, sell_currency_symbol, sell_amount, fee=FEE, timestamp=now()):
        self.update_balance(buy_currency_symbol, buy_amount)
        self.append_ledger("trade", buy_currency_symbol, buy_amount, fee, timestamp,
                           self.get_balance(buy_currency_symbol))

        self.update_balance(sell_currency_symbol, -1 * sell_amount)
        self.append_ledger("trade", sell_currency_symbol, -1 * sell_amount, fee, timestamp,
                           self.get_balance(sell_currency_symbol))

    def update_balance(self, currency_symbol, amount):
        self.set_balance(currency_symbol, self.get_balance(currency_symbol) + amount)

    def append_ledger(self, operation_type, currency_symbol, amount, fee, timestamp, balance):
        ledger_id = self.get_ledger_id()
        self.ledger.append([ledger_id, operation_type, currency_symbol, amount, fee, timestamp, balance])

    def get_ledger_id(self):
        self.update_ledger_id()
        return self.ledger_id

    def update_ledger_id(self):
        self.ledger_id = self.ledger_id + 1

    def get_balance(self, currency_symbol):
        return 0 if currency_symbol not in self.balances else self.balances[currency_symbol]

    def set_balance(self, currency_symbol, amount):
        self.balances[currency_symbol] = amount
