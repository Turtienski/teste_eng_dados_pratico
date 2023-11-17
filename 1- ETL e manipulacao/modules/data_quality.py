import numpy as np
import pandas as pd

class DataQuality:

	def __init__(self, data):
		self.data = data

	def remove_duplicates(self):
		duplicated_rows = self.data[self.data.duplicated()]
		self.data.drop_duplicates(inplace=True) # modify df rather than create a new one
		return len(duplicated_rows.index)

	def handle_missing_values(self):
		# use average product sales when value is missing
		sale_value_mean = self.data.groupby('product_id', as_index=False)['sale_value'].mean()
		data_aux = self.data.fillna(sale_value_mean)
		self.data['sale_value'] = data_aux['sale_value']

		# use default date '1900-01-01' to report that the data is missing
		self.data['date'] = self.data['date'].fillna(pd.to_datetime('1900-01-01'))
		self.data['transaction_id'] = self.data.transaction_id.ffill()+self.data.groupby(self.data.transaction_id.notnull().cumsum()).cumcount()

		# fill the remaining numeric columns with 0
		numeric_columns = self.data.select_dtypes(include=['number']).columns
		self.data[numeric_columns] = self.data[numeric_columns].fillna(0)

		# fill the remaining object columns with 'NI' (not informed)
		self.data = self.data.fillna('NI')
		return self.data

	def check_data_rules(self):
		error = pd.DataFrame()

		# check if all columns exists in data
		header = ['transaction_id', 'date', 'product_id', 'seller_id', 'sale_value', 'currency']
		columns = list(self.data.columns.values)
		diff = list(set(header) - set(columns))
		if diff:
			s = ', '.join(diff)
			error = {'Type':['header'], 'msg':["Columns not found: "+s]}
			error = pd.DataFrame(error)
			return error

		# sale_value must be greater than 0
		if (self.data['sale_value'] < 0).any():
			error = self.data.loc[self.data['sale_value'] < 0]
			error['type'] = 'sale_value'

		# transaction_id must be unique
		if not self.data['transaction_id'].is_unique:
			error_aux = self.data[self.data.duplicated('transaction_id', keep=False) == True]
			error_aux['type'] = 'transaction_id'
			error = pd.concat([error, error_aux])

		# file must have valid date values
		notnull = self.data['date'].notnull()
		not_datetime = pd.to_datetime(self.data['date'], errors='coerce').isna()
		not_datetime = not_datetime & notnull
		if (not_datetime == True).any():
			error_aux = self.data.loc[not_datetime == True]
			error_aux['type'] = 'date'
			error = pd.concat([error, error_aux])

		return error

	def data_transformation(self):
		# converting FICT to USD
		self.data['sale_value'] = np.where(self.data['currency'] == 'FICT', self.data['sale_value'] * 0.75, self.data['sale_value'])
		self.data['currency'] = np.where(self.data['currency'] == 'FICT', 'USD', self.data['currency'])
		return self.data