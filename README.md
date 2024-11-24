# ABODA CHALLENGE API

Esta API foi construída como parte do processo seletivo da Aboda. Ela permite o upload de arquivos CSV contendo dados de preços de ações, bem como a consulta de várias estatísticas relacionadas a esses dados.

## Funcionalidades

- Upload de arquivos CSV contendo dados de preços de ações.
- Consulta de volume negociado mais alto.
- Consulta do preço de fechamento mais baixo.
- Consulta do preço médio diário.
- Consulta da variação percentual diária.
- Consulta de tabela com os dados consolidados.

## Como Executar o Projeto

### Pré-requisitos

- Docker e Docker Compose instalados na máquina.

### Passos para Rodar

1. Clone o repositório:

   ```sh
   git clone https://github.com/Andrew1302/Aboda
   ```
2. Navegue até o diretório do projeto

   ```sh
   cd Aboda\desafio-API
   ```
3. Construa e inicie o Docker

   ```sh
   docker-compose up --build
   ```
4. Acesse a API em seu navegador

   [http://localhost:8000/](http://localhost:8000)
