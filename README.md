
Este repositório contém scripts em Python para processar dados financeiros relacionados a fundos e carteiras. Ele inclui funcionalidades para calcular a participação acionária dos investidores em fundos, calcular o valor do patrimônio imobiliário de carteiras e gerar arquivos Excel com os dados processados.

## Funcionalidades

O script realiza as seguintes tarefas:

1. **Carregar a configuração**: Lê um arquivo de configuração INI para definir caminhos e outras configurações importantes.
1. **Interpretar arquivso XML**: Lê todos os arquivos com extensão xml no diretório e seus subdiretórios informado no arquivo de configuração INI.
1. **Calcular participação acionária (Equity Stake)**: Calcula a participação de cada investidor com base na quantidade disponível de cotas e o valor dos fundos.
1. **Calcular valor do patrimônio imobiliário**: Para carteiras, calcula o valor do patrimônio imobiliário com base na porcentagem de participação e no valor contábil.
1. **Processar dados de fundos e carteiras**: Lê os arquivos Excel contendo dados brutos de fundos e carteiras, realiza os cálculos necessários e salva os resultados em novos arquivos Excel.

## Pré-requisitos

Para rodar o script, você precisa ter o Python 3.x instalado e as seguintes dependências:

- pandas
- openpyxl (para ler e escrever arquivos Excel)

Instale as dependências executando o comando:

```bash
pip install pandas openpyxl
```

## Estrutura de Arquivos

A estrutura de diretórios do repositório é a seguinte:

```
/config.ini                   # Arquivo de configuração
```

## Como Usar

1. **Configuração**: Antes de rodar o script, edite o arquivo `config.ini` para definir os caminhos dos arquivos de entrada e saída, conforme necessário.

1. **Execução**: Execute o script principal para processar os dados. O script irá ler os arquivos de entrada, calcular os valores necessários e gerar os arquivos de saída.

Para rodar o script, execute o seguinte comando:

```bash
python script.py
```

## Funções Principais

### `load_config(config_file)`
Carrega as configurações a partir de um arquivo INI.

### `compute_equity_stake(df_investor, df_invested)`
Calcula a participação acionária de cada investidor com base nas cotas disponíveis e nos valores dos fundos.

### `compute_equity_real_state(df_investor)`
Calcula o valor do patrimônio imobiliário de uma carteira de investimentos com base na porcentagem de participação e no valor contábil.

### `get_text_columns_carteiras()`
Retorna uma lista das colunas que devem ser tratadas como texto nos dados de carteiras.

### `get_text_columns_fundos()`
Retorna uma lista das colunas que devem ser tratadas como texto nos dados de fundos.

## Contribuições

Sinta-se à vontade para contribuir com melhorias ou correções. Basta fazer um fork do repositório, fazer suas alterações e enviar um pull request.

## Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para mais detalhes.
