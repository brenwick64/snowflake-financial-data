import pandas as pd

class TCBKController:
    def __init__(self):
        self.target_schema = ['SOURCE_SYSTEM', 'ACCOUNT_NUMBER', 'DATE', 'DESCRIPTION', 'TYPE', 'AMOUNT', 'CATEGORY', 'BALANCE']
        self.source_system = 'TCBK'
        self.account_number = '2007'
        
    def __get_amount(self, credit, debit):
        return credit if credit else debit
    
    def __get_type(self, credit, debit):
        return 'credit' if credit else 'debit'
        
    def create_transaction(self, statement_df):
        transactions_df = pd.DataFrame(columns=self.target_schema)
        transactions_df['DESCRIPTION'] = statement_df['DESCRIPTION']
        transactions_df['DATE'] = statement_df['POST_DATE']
        transactions_df['SOURCE_SYSTEM'] = self.source_system
        transactions_df['ACCOUNT_NUMBER'] = self.account_number
        transactions_df['TYPE'] = statement_df.apply(lambda x: self.__get_type(x['CREDIT'], x['DEBIT']), axis=1)
        transactions_df['AMOUNT'] = statement_df.apply(lambda x: self.__get_amount(x['CREDIT'], x['DEBIT']), axis=1)
        transactions_df['BALANCE'] = statement_df['BALANCE']
        transactions_df['CATEGORY'] = statement_df['CATEGORY']     
        
        return transactions_df