import pandas as pd
import time
import pytest
from unittest.mock import MagicMock

# Para o teste serão utilizados alguns data frames Pandas
df1 = pd.DataFrame({'A': [1, 2, 3], 'B': [2, 3, 5], 'C': [5, 7, 9]}) # Iguais e sem nulos happy path
df2 = pd.DataFrame({'A': [1, 2, 3], 'B': [2, 3, 5], 'C': [5, 7, 9]}) # Iguais e sem nulos happy path
df3 = pd.DataFrame({'A': [1, None, 3], 'B': [2, 3, None], 'C': [None, 7, 9]}) # Apresenta nulos, mesmo número de linhas (borda)
df4 = pd.DataFrame({'A': [1, None, 3], 'B': [2, 3, None], 'C': [None, 7, 9]}) # Apresenta nulos, mesmo número de linhas (borda)
df5 = pd.DataFrame({'A': [1, 2], 'B': [2, 3], 'C': [5, 7]}) # Diferente quantidade de linhas (Erro!)
df6 = pd.DataFrame({'A': [1, 2, 3], 'B': [2, 3, 5], 'C': [5, 7, 9]}) # Diferente quantidade de linhas (Erro!)
df7 = pd.DataFrame({'A': [1, 2, None], 'B': [2, 3, None], 'C': [5, 7, None]}) # Diferente quantidade de nulo, mesmo número de linhas (Erro!)
df8 = pd.DataFrame({'A': [1, 2, 3], 'B': [2, 3, 5], 'C': [5, 7, 9]}) # Diferente quantidade de nulo, mesmo número de linhas (Erro!)

def verifica_tb_extract(df_extracted, df_loaded):
    aproved = 0
    # Primeiro, contagens
    if len(df_extracted.index) == len(df_loaded.index):
        print('Contagens iguais: ', len(df_extracted.index))
        aproved += 1
    else:
        print('Alerta! Contagens não batem, extracted: ', len(df_extracted.index), 'loaded: ', len(df_loaded.index))
    
    # Verificando se a quantidade de nulos, em cada coluna, bate
    if (df_extracted.isna().sum() == df_loaded.isna().sum()).sum()/len(df_extracted.columns) == 1.0:
        print("Quantidades de nulo iguais: \n", df_extracted.isna().sum())
        aproved += 1
    else:
        print("Quantidades de nulo desiguais! Verifique as colunas: \n", 
              (df_extracted.isna().sum() == df_loaded.isna().sum()))
    
    return True if aproved == 2 else False

def test_verifica_tb_extract():
    actual_result = verifica_tb_extract(df7, df8)
    assert actual_result == True