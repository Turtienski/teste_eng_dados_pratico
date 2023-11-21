import pytest
import pandas as pd
import sys

# a sales_data.csv copy was created and modified to have some problems:
# 1- duplicated data;
# 2- missing values;
# 3- not valid types

# It's important to verify if all DataQuality rules are working as expecting

sys.path.insert(1, 'C:\\Users\\de_go\\Desktop\\Denise\\teste_eng_dados_pratico\\1- ETL e manipulacao')
from modules.data_quality import DataQuality

df = pd.read_csv("C:\\Users\\de_go\\Desktop\\Denise\\teste_eng_dados_pratico\\1- ETL e manipulacao\\data\\sales_data - modificado.csv")
dq = DataQuality(df)

# the file has one duplicated row, so, the remove_duplicates return should be 1
def test_remove_duplicates():
	assert dq.remove_duplicates() == 1

# the file has a missing date on index 17, the handle_missing_values function should return "1900-01-01"
def test_handle_missing_values():
	df2 = dq.handle_missing_values()
	assert df2['date'].iloc[17] == pd.to_datetime("1900-01-01")

# the file has a stranger date format, so, the df containg the errors should not be empty"
def test_check_data_rules():
	df3 = dq.check_data_rules()
	assert not df3.empty

# this is a check if the currency is being correcty converted
def test_data_transformation():
	df4 = dq.data_transformation()
	assert df4['sale_value'].iloc[2] == df['sale_value'].iloc[2]*0.75