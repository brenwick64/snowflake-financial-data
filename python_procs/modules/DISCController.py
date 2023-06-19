import pandas as pd

class DISCController:
    def __init__(self):
        self.target_schema = ['ACCOUNT_NUMBER', 'DATE', 'DESCRIPTION', 'TYPE', 'AMOUNT', 'BALANCE']
        self.account_number = '1111'
        
    def __get_amount(self, amount):
        if amount:
            return abs(float(amount.replace(',', '')))
        return None
    
        
    def create_transaction(self, statement_df):
        transactions_df = pd.DataFrame(columns=self.target_schema)
        transactions_df['DESCRIPTION'] = statement_df['DESCRIPTION']
        transactions_df['DATE'] = statement_df['TRANSACTION_DATE']
        transactions_df['ACCOUNT_NUMBER'] = self.account_number
        transactions_df['TYPE'] = 'debit'
        transactions_df['AMOUNT'] = statement_df.apply(lambda x: self.__get_amount(x['AMOUNT']), axis=1)
        transactions_df['BALANCE'] = None

        return transactions_df