import pandas as pd

class BOAController:
    def __init__(self):
        self.target_schema = ['ACCOUNT_NUMBER', 'DATE', 'DESCRIPTION', 'TYPE', 'AMOUNT', 'BALANCE']
        self.account_number = '0000'
        
    def __get_amount(self, amount):
        if amount:
            return abs(float(amount.replace(',', '')))
        return None
    
    def __get_type(self, amount):
        if amount and amount[0] == '-':
            return 'debit'
        if amount and amount[0] != '-':
            return 'credit'
        return None  
    
    def __get_balance(self, balance):
        if balance:
            return float(balance.replace(',', ''))
        return None
    
        
    def create_transaction(self, statement_df):
        transactions_df = pd.DataFrame(columns=self.target_schema)
        transactions_df['DESCRIPTION'] = statement_df['DESCRIPTION']
        transactions_df['DATE'] = statement_df['DATE']
        transactions_df['ACCOUNT_NUMBER'] = self.account_number
        transactions_df['TYPE'] = statement_df.apply(lambda x: self.__get_type(x['AMOUNT']), axis=1)
        transactions_df['AMOUNT'] = statement_df.apply(lambda x: self.__get_amount(x['AMOUNT']), axis=1)
        transactions_df['BALANCE'] = statement_df.apply(lambda x: self.__get_balance(x['RUNNING_BALANCE']), axis=1)
        
        # Removes null amount rows (bad data)      
        return transactions_df[transactions_df['AMOUNT'].notna()]