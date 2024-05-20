# Desafio de Mercado para "5GFlix"
## Introdução

Participei de um desafio interessante proposto pela "5GFlix", um novo aplicativo de streaming que busca definir sua estratégia de negócios através de análises de mercado. O objetivo foi analisar filmes e séries disponíveis em duas plataformas concorrentes, Amazon e Netflix, utilizando dados fornecidos por elas. O CTO da "5GFlix", Alan Turing, solicitou à Solvimm a criação de uma estrutura lógica que permitisse ao time de BI responder a várias perguntas de negócio relacionadas a esses dados.

## Objetivos

Para realizar essas análises, foram fornecidas duas bases de dados:

- Netflix: Netflix Prize Data
- Amazon: Amazon Customer Reviews Dataset
- Subconjuntos Considerados:
  - Video_v1_00
  - Video_DVD_v1_00
  - Digital_Video_Download_v1_00

## Solução Implementada

Para resolver o desafio, utilizei a seguinte abordagem técnica:

- PySpark: Utilizado para a extração, limpeza e carregamento das massivas quantidades de dados de avaliações provenientes da Amazon e da Netflix. Essa escolha foi crucial para lidar com grandes volumes de dados de forma eficiente.
- Amazon S3 Bucket: Implementado para a fase de staging, proporcionando uma solução de armazenamento escalável e durável para os dados brutos.
- Snowflake: Utilizado para a implementação do estágio de staging, criação de tabelas no Data Warehouse e para a transformação e carregamento dos dados das fases de staging para as tabelas do Data Warehouse.
- 
## Resultado

Esta solução permitiu que os dados fossem processados e organizados de forma eficiente, garantindo que o time de BI da "5GFlix" pudesse realizar análises detalhadas e baseadas em dados precisos para formular suas estratégias de mercado.
