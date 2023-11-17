import pytest
import pandas as pd
import sys

sys.path.insert(1, 'C:\\Users\\de_go\\Desktop\\Denise\\teste_eng_dados_pratico\\1- ETL e manipulacao')
from modules.data_quality import DataQuality

df = pd.read_csv("C:\\Users\\de_go\\Desktop\\Denise\\teste_eng_dados_pratico\\1- ETL e manipulacao\\data\\sales_data - modificado.csv")
dq = DataQuality(df)

# error or exception

def test_remove_duplicates():
	assert dq.remove_duplicates() == 1

def test_handle_missing_values():
	df2 = dq.handle_missing_values()
	assert df2['date'].iloc[17] == pd.to_datetime("1900-01-01")

def test_check_data_rules():
	df3 = dq.check_data_rules()
	assert not df3.empty

# happy path

def test_data_transformation():
	df4 = dq.data_transformation()
	assert df4['sale_value'].iloc[2] == df['sale_value'].iloc[2]*0.75