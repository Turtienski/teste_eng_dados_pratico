Proposta de Arquitetura na AWS para Coletar e Processar Dados de Diferentes Fontes

1. Coleta de Dados:

AWS API Gateway:
Criação de uma API usando o AWS API Gateway para receber os dados de diferentes fontes externas.
A API pode ser configurada para receber requisições HTTP ou eventos de outras fontes, como S3, Kinesis, entre outras.
É possível definir recursos, métodos e parâmetros para mapear os dados recebidos para um formato padronizado.

AWS Lambda:
Criação de funções Lambda serverless para processar e transformar os dados recebidos pela API em tempo real.
Cada função Lambda pode ser responsável por um tipo específico de processamento ou transformação de dados.

AWS Kinesis Data Firehose:
Configuração de um delivery stream no AWS Kinesis Data Firehose para receber os dados processados pelas funções Lambda.
A escolha do Kinesis Data Firehose garante a entrega confiável e escalável dos dados.



2. Processamento em Tempo Real:

AWS Lambda:
As funções Lambda processam e transformam os dados recebidos em tempo real.
Por exemplo, as funções podem realizar a limpeza dos dados, removendo duplicatas e tratando valores ausentes.
As funções também podem realizar outras transformações, como normalização de dados e agregações simples.



3. Armazenamento de Dados:
Amazon S3:
Configuração de um bucket no Amazon S3 para armazenar os dados brutos (antes do processamento) em formato de data lake.
Os dados brutos são armazenados em um local centralizado e podem ser acessados facilmente para análises futuras.

Amazon Redshift ou Amazon RDS:
Configuração de um banco de dados relacional no Amazon Redshift ou Amazon RDS para armazenar os dados processados e transformados.
O Amazon Redshift seria mais adequado para consultas analíticas complexas.
O Amazon RDS oferece opções de banco de dados relacional, como PostgreSQL, MySQL, etc., dependendo das necessidades do projeto pode ser a melhor escolha.



4. Observabilidade:
Amazon CloudWatch:
Configuração do Amazon CloudWatch para monitorar e coletar métricas sobre o desempenho do pipeline e dos serviços envolvidos.
O CloudWatch poderia coletar métricas de diferentes serviços da AWS, como Lambda, Kinesis Data Firehose e Redshift/RDS.
As métricas coletadas incluiriam o número de registros processados, latência, erros, entre outros.

Grafana:
Utilização do Grafana para visualizar as métricas e criar dashboards personalizados com base nos dados coletados pelo CloudWatch.
A equipe poderia utilizar o Grafana para identificar tendências, padrões e possíveis problemas no pipeline.



5. Testes Unitários:
Implementação de testes unitários utilizando o Pytest para garantir a robustez e confiabilidade das funções Lambda e dos processos ETL.
Os testes devem cobrir casos padrão, casos de borda e situações de erro ou exceção para validar o comportamento esperado do código.



Essa arquitetura proposta procura garantir a coleta eficiente de dados de diferentes fontes, o processamento em tempo real para limpeza e transformação, o armazenamento seguro e escalável dos dados brutos e processados, além da observabilidade e monitoramento contínuo do pipeline. A preferência pela utilização de serviços gerenciados da AWS permite reduzir a complexidade operacional e garantir a resiliência do sistema, possibilitando uma abordagem mais ágil e focada na entrega de valor.
