import pandas as pd

class DISCController:
    def __init__(self):
        self.target_schema = ['SOURCE_SYSTEM', 'ACCOUNT_NUMBER', 'DATE', 'DESCRIPTION', 'TYPE', 'AMOUNT', 'CATEGORY', 'BALANCE']
        self.source_system = 'DISC'
        self.account_number = '1111'
        
    def __get_amount(self, amount):
        if amount:
            return abs(float(amount.replace(',', '')))
        return None
    
    def __get_balance(self, amount):
        if amount:
            return float(amount.replace(',', ''))
        return None
    
        
    def create_transaction(self, statement_df):
        transactions_df = pd.DataFrame(columns=self.target_schema)
        transactions_df['DESCRIPTION'] = statement_df['DESCRIPTION']
        transactions_df['DATE'] = statement_df['TRANSACTION_DATE']
        transactions_df['ACCOUNT_NUMBER'] = self.account_number
        transactions_df['TYPE'] = 'debit'
        transactions_df['SOURCE_SYSTEM'] = self.source_system
        transactions_df['AMOUNT'] = statement_df.apply(lambda x: self.__get_amount(x['AMOUNT']), axis=1)
        transactions_df['CATEGORY'] = statement_df['CATEGORY']
        transactions_df['BALANCE'] = statement_df.apply(lambda x: self.__get_balance(x['BALANCE']), axis=1)

        return transactions_df