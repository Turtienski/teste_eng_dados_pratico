# Explicação Arquitetura


<p align="center">
  <img src="./architecture_solution/arquitetura-processamento-stream.png" />
</p>

Para a solução pensei em ter um job **python** que pode ser agendado e chamado por uma dag **airflow**, onde ele busca os dados e joga em um tópico do **kafka**, esses dados precisam ser salvos **as is** então jogamos em um bucket em uma **camada bronze** ao mesmo tempo que jogamos a informação para um job **spark streaming** fazer o processamento e tratamento desses dados e salvar os dados em uma base de dados **NOSQL de alta disponibilidade** no exemplo usei o banco **cassandra** que podemos chamar de **camada ouro**.

Podemos fazer o monitoramento desses jobs e rastreio de erros e warnings usando o cloudwatch ou alguma outra plataforma que ajude a equipe a ter uma visibilidade do que acontece no pipeline.