# Teste Prático para Engenheiros de Dados - Itaú RTDA

## Introdução

Este teste prático é destinado a avaliar suas habilidades como Engenheiro de Dados. Cada seção abaixo descreve uma tarefa específica que você deve completar.

## Instruções para Fork

Antes de começar, faça um **fork** deste repositório para sua conta pessoal no GitHub. Após completar todas as tarefas, crie um **Pull Request** para este repositório original. As suas soluções serão revisadas através deste PR.

## Desafios

### 1. ETL e Manipulação de Dados

#### Descrição

Utilize o arquivo `sales_data.csv` e:

- Limpe os dados removendo linhas duplicadas e tratando valores ausentes.
- Transforme o valor da venda de uma moeda fictícia para USD usando a taxa de conversão de 1 FICT = 0.75 USD.
- Carregue os dados limpos e transformados em um banco de dados relacional.

### 2. Análise com Apache Spark(utilize PySpark ou Spark)

#### Descrição

Dado um conjunto fictício de logs `website_logs.csv`:

- Identifique as 10 páginas mais visitadas.
- Calcule a média de duração das sessões dos usuários.
- Determine quantos usuários retornam ao site mais de uma vez por semana.

### 3. Desenho de Arquitetura

#### Descrição

Proponha uma arquitetura em AWS para coletar dados de diferentes fontes:

- Desenhe um sistema para coletar dados de uma API.
- Processe esses dados em tempo real.
- Armazene os dados para análise futura.

### 4. Codificação

#### Descrição

Escreva um script Python para:

- Se conectar à [API de previsão do tempo OpenWeatherMap](https://openweathermap.org/api).
- Coletar dados dessa API para uma cidade de sua escolha.
- Armazenar os dados coletados em um banco de dados relacional ou NoSQL(de sua escolha).

### 5. Data Quality & Observability

#### Descrição

A qualidade dos dados é fundamental para garantir que as análises e os insights derivados sejam confiáveis. Observabilidade, por outro lado, refere-se à capacidade de monitorar e entender o comportamento dos sistemas. Para este desafio:

- Utilize o arquivo `sales_data.csv` e implemente verificações de qualidade de dados. Por exemplo:
  - Verifique se todos os IDs de usuários são únicos.
  - Confirme se os valores de vendas não são negativos.
  - Garanta que todas as entradas tenham timestamps válidos.
  - Quantidade de linhas ingeridas no banco de dados de sua escolha é igual a quantidade de linhas originais
- Crie métricas de observabilidade para o processo ETL que você desenvolveu anteriormente(não é necessário implementação):
  - Monitore o tempo que leva para os dados serem extraídos, transformados e carregados.
  - Implemente alertas para qualquer falha ou anomalia durante o processo ETL.
  - Descreva como você rastrearia um problema no pipeline, desde o alerta até a fonte do problema.
---

### 6. Teste Unitário

#### Descrição

Os testes unitários são fundamentais para garantir a robustez e confiabilidade do código, permitindo identificar e corrigir bugs e erros antes que eles atinjam o ambiente de produção. Para este desafio:

- Escolha uma das funções ou classes que você implementou nas etapas anteriores deste teste.
- Escreva testes unitários para esta função ou classe. Os testes devem cobrir:
  - Casos padrão ou "happy path".
  - Casos de borda ou extremos.
  - Situações de erro ou exceção.
- Utilize uma biblioteca de testes de sua escolha (como `pytest`, `unittest`, etc.).
- Documente os resultados dos testes e, caso encontre falhas através dos testes, descreva como as corrigiria.

Dica: Valorizamos a cobertura de código, mas também a relevância e qualidade dos testes. Não é apenas sobre escrever muitos testes, mas sobre escrever testes significativos que garantam a confiabilidade do sistema.

# O que é esperado do candidato

Caro candidato, o teste prático proposto visa avaliar suas habilidades, competências e abordagem como Engenheiro de Dados. Aqui está o que esperamos de você:

## 1. Atenção aos Detalhes

Verifique cuidadosamente cada etapa do teste, garantindo que nenhum detalhe foi perdido. Em Engenharia de Dados, muitas vezes os detalhes são cruciais para o sucesso de um projeto.

## 2. Qualidade do Código

Esperamos que o código que você produza seja claro, legível e bem organizado. Isso inclui:
- Uso adequado de funções, classes e módulos.
- Comentários relevantes.
- Nomes significativos para variáveis e funções.

## 3. Eficiência

Enquanto a qualidade do código é importante, também estamos interessados em ver como você aborda problemas de eficiência. Considere a otimização de seu código, especialmente em tarefas que envolvem grandes conjuntos de dados.

## 4. Abordagem Analítica

Queremos ver sua habilidade em transformar dados brutos em insights úteis. Isso não significa apenas escrever código, mas entender e interpretar os resultados.

## 5. Conhecimento em Ferramentas e Plataformas

O teste foi desenhado para avaliar seu conhecimento em ferramentas específicas como Apache Spark e AWS. Mostre-nos que você sabe como usar essas ferramentas eficazmente para resolver problemas.

## 6. Proposta de Solução

Na seção de Desenho de Arquitetura, estamos interessados em sua capacidade de projetar sistemas robustos e escaláveis. Sua solução deve considerar aspectos como escalabilidade, resiliência, custo e manutenibilidade.

## 7. Autonomia

Embora esteja livre para pesquisar e procurar referências, queremos ver sua capacidade de trabalhar de forma autônoma e resolver problemas com os recursos que possui.

## 8. Comunicação

Ao finalizar o teste, você será avaliado não apenas pelas soluções técnicas, mas também por sua capacidade de comunicar suas escolhas, decisões e resultados. Esteja preparado para justificar suas decisões e explicar seu raciocínio.

Lembre-se, este teste não é apenas sobre acertar ou errar, mas sobre mostrar suas habilidades, abordagem e paixão pela Engenharia de Dados. Estamos ansiosos para ver o que você pode fazer!
---

*"A excelência não é um destino, mas uma jornada contínua!"* - Brian Tracy

Boa sorte! 🚀
