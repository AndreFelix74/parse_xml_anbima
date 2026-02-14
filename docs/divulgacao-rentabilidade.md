# Pipeline de Cálculo e Reconciliação de Rentabilidades

Este pipeline é utilizado para **atualizar os dados de rentabilidade divulgados pela Gestora** (Vivest, no sistema **Sofia**).

---

- **O quê?**
  Um processo automatizado que carrega dados internos de rentabilidade (`mec_sac`), integra informações auxiliares, calcula indicadores (mensais, YTD, agregados) e reconcilia entidades e valores com a API Maestro.

- **Por quê?**
  Para garantir que os dados de rentabilidade divulgados sejam **precisos, consistentes e alinhados** tanto com os cálculos internos do sistema Sofia quanto com os valores oficiais disponibilizados pela API Maestro.

- **Quem?**
  - **Pessoas usuárias:** equipe de tecnologia e de investimentos da Gestora.
  - **Sistemas envolvidos:**
    - **mec_sac**: sistema terceirizado que fornece os dados de movimentação e posições de carteira.  
    - **Maestro**: sistema interno utilizado como repositório oficial das informações de investimentos e rentabilidades.  
  - **Destinatários:** área de comunicação e partes interessadas externas que acessam os dados divulgados.

- **Onde?**
  O pipeline roda no ambiente interno do sistema Sofia, consumindo dados armazenados em diretórios específicos e integrando-se à API Maestro via internet.

- **Quando?**
  Executado mensalmente para atualizar o site da Gestora.

- **Como?**
  1. Carregando arquivos `mec_sac` e dados auxiliares.
  1. Calculando rentabilidades agregadas (mensal, anual, consolidada).
  1. Reconciliando entidades locais com IDs da API Maestro.
  1. Comparando rentabilidades internas com dados oficiais da API.
  1. Exportando resultados em arquivos CSV prontos para divulgação.

- **Quanto custa?**
  O custo envolve **recursos computacionais internos** (processamento e armazenamento) e manutenção do código. Não há custo adicional por transação na API.

---

## Configuração do ambiente

Para executar o pipeline, é necessário preparar o ambiente com as dependências do Python, variáveis de ambiente e arquivos de configuração.

### Dependências de software
- **Python 3.10+**
- Bibliotecas Python:
  - `pandas`
  - `requests`
  - `concurrent.futures` (padrão da stdlib)

### Arquivos de configuração
- **`config.ini`**  
  Define os caminhos usados pelo pipeline:
  - `[Paths]`
    - `xlsx_destination_path`: diretório de saída dos arquivos divulgados.
    - `data_aux_path`: diretório com dados auxiliares (ex.: cadastros).
    - `mec_sac_path`: diretório com os arquivos de entrada mec_sac.
  - `[Debug]`: seção obrigatória para habilitar parâmetros de depuração.

### Variáveis de ambiente
O pipeline depende das seguintes variáveis para autenticação no Azure AD e comunicação com a API Maestro:

- `TENANT_ID`: identificador do tenant Azure.  
- `CLIENT_ID`: ID da aplicação registrada no Azure AD.  
- `CLIENT_SECRET`: segredo da aplicação.  
- `SCOPE`: escopo para obtenção do token OAuth2.  
- `API_BASE`: URL base da API Maestro.  

Essas variáveis podem ser definidas manualmente no shell ou carregadas via script.

### Scripts de exportação das variáveis
Na pasta `data_io/` existe um script de exemplo para exportar as variáveis:

**`data_io/export-maestro.sh`**
```bash
export TENANT_ID=""
export CLIENT_ID=""
export CLIENT_SECRET=""
export SCOPE=""
export API_BASE=""
```
Para carregar as variáveis no shell atual, use:
```bash
source data_io/export-maestro.sh
```

---

## Etapas do pipeline

### 1. Carregamento da configuração
- O arquivo `config.ini` define os caminhos para:
  - Pasta de saída dos arquivos (`xlsx_destination_path`).
  - Dados auxiliares (`data_aux_path`).
  - Arquivos `mec_sac` (`mec_sac_path`).
- O módulo valida se as seções `[Debug]` e `[Paths]` estão presentes.

### 2. Descoberta e leitura dos arquivos mec_sac
- Busca recursiva por arquivos `_mecSAC_*.xlsx`.
- Cada arquivo encontrado é carregado em paralelo usando `ProcessPoolExecutor`.
- Os resultados são concatenados em um único `DataFrame`.

### 3. Cálculo das rentabilidades agregadas
- Mescla os dados `mec_sac` com informações de `dCadPlanoSAC`.
- Calcula:
  - Rentabilidades mensais ponderadas por **TIPO_PLANO, GRUPO, INDEXADOR** e **consolidado**.
  - Rentabilidade acumulada no ano **Year To Date (YTD)**.
- Gera uma tabela única com colunas padronizadas (`TIPO`, `NOME`, `DT`, `RENTAB_MES`, `RENTAB_ANO`).

### 4. Reconciliação de entidades
- Consulta a API Maestro para obter listas oficiais de:
  - **Grupos**
  - **Indexadores**
  - **Planos**
  - **Tipos de Plano**
- Mapeia cada entidade do `rentab_mecsac` para o respectivo **ID da API**, preenchendo a coluna `api_id`.

### 5. Reconciliação de rentabilidades
- Consulta a API Maestro para obter:
  - Rentabilidades **mensais**
  - Rentabilidades **anuais**
- Realiza merges entre os dados locais e os da API para permitir comparação.

### 6. Exportação dos resultados
São gerados arquivos no formato CSV (ou outro definido):
- `divulga_rentab_agregados`: Rentabilidades calculadas internamente.
- `divulga_rentab_ids_comparados`: Dados locais com IDs reconciliados da API.
- `divulga_rentab_rentab_comparadas`: Comparação entre rentabilidades locais e Maestro.

### 7. Atualização do Maestro
- Em desenvolvimento
---

## Resumo

O pipeline garante:
1. **Automação** na leitura e preparação dos dados de rentabilidade.
1. **Consistência** entre as entidades locais e as entidades oficiais da API Maestro.
1. **Auditoria** ao gerar arquivos intermediários e finais que permitem comparar cálculos internos com os valores fornecidos pela API.

Esse processo atualiza **os indicadores de rentabilidade** divulgados pela Gestora.
