# Projeto Final — Data Engineering Programming

**Disciplina:** Data Engineering Programming — FIAP  
**Professor:** Marcelo Barbosa Pinto

Pipeline PySpark que cruza dados de **pedidos** e **pagamentos** para gerar um relatório dos pedidos recusados (`status = false`) classificados como legítimos (`fraude = false`). Saída em **Parquet** (Snappy).

---

## Objetivo

A alta gestão deseja identificar pedidos cujo pagamento foi **recusado** mas a avaliação de fraude os classificou como **legítimos** (ano de filtro configurável; padrão **2025**).

| Coluna            | Origem                                       |
| ----------------- | -------------------------------------------- |
| `id_pedido`       | `pedidos.id_pedido`                          |
| `uf`              | `pedidos.uf`                                 |
| `forma_pagamento` | `pagamentos.forma_pagamento`                 |
| `valor_total`     | `pedidos.valor_unitario × pedidos.quantidade` |
| `data_criacao`    | `pedidos.data_criacao` (na saída Parquet: string ISO via `date_format`) |

Ordenação: `uf` → `forma_pagamento` → `data_criacao` (no Spark, ordenação sobre o **timestamp** de `data_criacao` antes da conversão para string na saída).

---

## Arquitetura

Orientação a objetos com **injeção de dependências** na composição do pipeline: em `src/pipeline/cli.py`, a função `run_pipeline` instancia `AppConfig`, `SparkSessionManager`, readers, regra de negócio, `ParquetWriter` e `PipelineOrchestrator`. As entradas **`python main.py`** e **`python -m pipeline`** apenas chamam `run_pipeline`; `main.py` ainda coloca `src/` no `sys.path` quando você roda o script na raiz **sem** `pip install -e .`.

```
Projeto Final/
├── main.py                              # Entrada: sys.path + chama run_pipeline
├── src/
│   ├── config/app_config.py             # Configurações centralizadas
│   ├── spark/spark_session_manager.py   # Sessão Spark
│   ├── schemas/                         # Schemas explícitos (pedidos + pagamentos)
│   ├── data_io/reader.py / writer.py    # Leitura e escrita de dados
│   ├── business/relatorio_pedidos.py    # Regra de negócio
│   └── pipeline/
│       ├── cli.py                       # Composição (DI), CLI e run_pipeline
│       ├── pipeline_orchestrator.py     # Orquestração read → transform → write
│       └── __main__.py                  # python -m pipeline → run_pipeline
├── scripts/                             # setup_venv (CMD / PowerShell / Git Bash)
├── tests/
│   ├── conftest.py                      # Fixture SparkSession + fixtures de dados
│   ├── fixtures_datasets.py             # Gera gzip sintéticos para readers / relatório
│   ├── test_*_schema.py                 # Schemas pedidos / pagamentos
│   ├── test_*_reader.py                 # Readers
│   ├── test_parquet_writer.py
│   ├── test_relatorio_pedidos.py        # Regra de negócio
│   ├── test_app_config.py
│   ├── test_cli.py                      # PYSPARK_* / UTF-8 console (Windows)
│   ├── test_spark_session_manager.py
│   └── test_pipeline_orchestrator.py
└── data/
    ├── input/                           # Datasets (CSV.gz + JSON.gz)
    └── output/                          # ⬅ Resultado do pipeline (Parquet)
```

---

## Requisitos

| Requisito    | Versão                    |
| ------------ | ------------------------- |
| **Python**   | 3.9+ (testado em 3.12)   |
| **Java JDK** | 8, 11 ou 17              |

> O Java é exigência do Spark. Verifique com `java -version`.
## Instalação de requisitos do projeto


<details>
<summary><strong>macOS (Homebrew)</strong></summary>

```bash
brew install --cask temurin@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

Torne persistente adicionando os `export` ao `~/.zshrc`.
</details>

<details>
<summary><strong>Ubuntu/Debian</strong></summary>

```bash
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

Torne persistente adicionando os `export` ao `~/.bashrc`.
</details>

<details>
<summary><strong>Fedora/RHEL</strong></summary>

```bash
sudo dnf install -y java-17-openjdk-devel
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
```
</details>

<details>
<summary><strong>Windows</strong></summary>

<br>

**Instalador:** `winget` (Windows 10/11). Em muitas máquinas o PowerShell precisa ser **como administrador** na primeira instalação; no **Git Bash** ou **CMD** o `winget` costuma funcionar se estiver no `PATH`.

<details>
<summary><strong>PowerShell</strong></summary>

**Java (JDK 17)**

```powershell
winget install --id Microsoft.OpenJDK.17 --silent --accept-package-agreements --accept-source-agreements
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot", "User")
$env:Path += ";$env:JAVA_HOME\bin"
```

> Ajuste o caminho do `JAVA_HOME` conforme a versão instalada em `C:\Program Files\Microsoft\`.

> **No** Windows nativo, o Spark costuma precisar de `winutils.exe` e `hadoop.dll` em `C:\hadoop\bin` para operações de filesystem/permissão.

**winutils**

```powershell
New-Item -ItemType Directory -Path "C:\hadoop\bin" -Force | Out-Null
Invoke-WebRequest -Uri "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe" -OutFile "C:\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/hadoop.dll" -OutFile "C:\hadoop\bin\hadoop.dll"
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
$env:Path += ";C:\hadoop\bin"
```
</details>

<details>
<summary><strong>Git Bash (Windows)</strong></summary>

Use os **mesmos** destinos de pasta; no Bash, o disco `C:\` é `/c/`. Confira o nome exato da pasta do JDK em `/c/Program Files/Microsoft/`.

**Java (JDK 17)**

```bash
winget install --id Microsoft.OpenJDK.17 --silent --accept-package-agreements --accept-source-agreements
export JAVA_HOME="/c/Program Files/Microsoft/jdk-17.0.18.8-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
```

> **No** Windows nativo, o Spark costuma precisar de `winutils.exe` e `hadoop.dll` em `C:\hadoop\bin` para operações de filesystem/permissão.

**winutils**

```bash
mkdir -p /c/hadoop/bin
curl -fsSL -o /c/hadoop/bin/winutils.exe "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe"
curl -fsSL -o /c/hadoop/bin/hadoop.dll "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/hadoop.dll"
export HADOOP_HOME="/c/hadoop"
export PATH="$HADOOP_HOME/bin:$PATH"
```
</details>

<details>
<summary><strong>CMD</strong></summary>

**Java (JDK 17)**

```bat
winget install --id Microsoft.OpenJDK.17 --silent --accept-package-agreements --accept-source-agreements
setx JAVA_HOME "C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
setx HADOOP_HOME "C:\hadoop"
```

> Ajuste o caminho de `JAVA_HOME` se a pasta do JDK tiver outro nome. Após `setx`, abra um **novo** CMD. Se o comando `java` não for reconhecido, adicione manualmente `;%JAVA_HOME%\bin` e `;%HADOOP_HOME%\bin` à variável de usuário **Path** (Configurações → Variáveis de ambiente) ou use o bloco **PowerShell** acima, que ajusta o `Path` na sessão.

> **No** Windows nativo, o Spark costuma precisar de `winutils.exe` e `hadoop.dll` em `C:\hadoop\bin` para operações de filesystem/permissão.

**winutils** (requer `curl` no CMD — padrão no Windows 10/11)

```bat
mkdir C:\hadoop\bin 2>nul
curl -fsSL -o C:\hadoop\bin\winutils.exe "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe"
curl -fsSL -o C:\hadoop\bin\hadoop.dll "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/hadoop.dll"
```

> O `HADOOP_HOME` já foi definido no bloco **Java (JDK 17)** acima. Se o `curl` não existir, use o bloco **PowerShell** para baixar os arquivos ou copie os `Invoke-WebRequest` dali.

</details>

<br>

Para deixar `JAVA_HOME`, `HADOOP_HOME` e `PATH` permanentes: use **Variáveis de ambiente** do Windows (como no bloco PowerShell), `setx` (CMD) ou adicione os `export` ao `~/.bashrc` do Git Bash, se for sempre usar só esse terminal.

<br>

> Se o `winget` não for encontrado no **Git Bash** ou no **CMD**, abra o **PowerShell** só para a instalação do JDK e depois ajuste as variáveis conforme o caminho real da pasta.

</details>


---

## Instalação do ambiente (venv + pip)

> ATENÇÃO: Execute o comando de instalação do ambiente virtual na **raiz do repositório** (pasta onde está `pyproject.toml`).

<details>
<summary><strong>1 - Instalação automática (recomendado no Windows)</strong></summary>

### Instalação automática (recomendado no Windows)

Na **raiz do repositório** (pasta onde está `pyproject.toml`; os scripts `setup_venv` validam esse arquivo):

| Terminal    | Comando |
| ----------- | ------- |
| **CMD**     | `scripts\setup_venv.cmd` |
| **PowerShell** | `powershell -ExecutionPolicy Bypass -File .\scripts\setup_venv.ps1` |
| **Git Bash**   | `bash scripts/setup_venv.sh` |
</details>



<details>
<summary><strong>2 - Instalação manual</strong></summary>

<details>
<summary><strong>- 2.1 - Linux / macOS (manual)</strong></summary>

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```
</details>

<details>
<summary><strong>- 2.2 - Windows — PowerShell (manual)</strong></summary>

Ordem **obrigatória**: política de execução → criar venv → ativar → instalar com `python -m pip`.

```powershell
cd "C:\caminho\para\Data-Engineering-Programming---Trabalho-Final-PySpark---FIAP"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

- `Set-ExecutionPolicy -Scope Process` vale só para **esta** janela do PowerShell (não exige administrador).
- Se preferir não usar `.ps1`, use o fluxo do **CMD** com `activate.bat` (abaixo).

</details>

<details>
<summary><strong>- 2.3 - Windows — CMD (manual, sem ExecutionPolicy)</strong></summary>

```bat
cd /d "C:\caminho\para\Data-Engineering-Programming---Trabalho-Final-PySpark---FIAP"
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

</details>

<details>
<summary><strong>- 2.4 - Windows — Git Bash (manual)</strong></summary>

Se **já existir** `.venv` (de outro terminal), **não** rode `python -m venv .venv` de novo no Git Bash — pode dar `Permission denied` em `python.exe`. Ative e instale:

```bash
cd "/c/caminho/para/Data-Engineering-Programming---Trabalho-Final-PySpark---FIAP"
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Só na **primeira** vez (sem pasta `.venv`):

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

</details>
</details>
<br>
 
> A primeira instalação baixa o PySpark (~300 MB). Pode levar alguns minutos.

---

## Datasets

Os arquivos **não** vêm no clone deste repositório: é preciso clonar os dois repositórios do professor **dentro de** `data/input/` (na raiz do projeto):

<details>
<summary><strong>Git Bash</strong></summary>

```bash
mkdir -p data/input
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data/input/datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data/input/dataset-json-pagamentos
```

</details>

<details>
<summary><strong>PowerShell</strong></summary>

```powershell
New-Item -ItemType Directory -Path "data\input" -Force | Out-Null
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data\input\datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data\input\dataset-json-pagamentos
```

</details>

<details>
<summary><strong>CMD</strong></summary>

```bat
mkdir data\input 2>nul
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data\input\datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data\input\dataset-json-pagamentos
```

</details>

<br>

| Dataset      | Repositório                                              | Caminho local esperado                                   |
| ------------ | -------------------------------------------------------- | ------------------------------------------------------- |
| **Pedidos**  | [datasets-csv-pedidos](https://github.com/infobarbosa/datasets-csv-pedidos)   | `data/input/datasets-csv-pedidos/data/pedidos/`         |
| **Pagamentos** | [dataset-json-pagamentos](https://github.com/infobarbosa/dataset-json-pagamentos) | `data/input/dataset-json-pagamentos/data/pagamentos/`   |

Sem esses clones, o pipeline encerra com mensagem explícita (não dependa só do erro genérico `[PATH_NOT_FOUND]` do Spark).

---

## Executar o Pipeline

Com o ambiente virtual **ativo** e na **raiz do projeto** (onde está `main.py`):

**PowerShell / CMD / Git Bash**

```bash
python main.py
```

Exemplo informando o ano explicitamente via CLI:

```bash
python main.py --ano-filtro 2025
```

Também é possível usar o alias:

```bash
python main.py --ano 2025
```

Após instalar o projeto em modo editável (`pip install -e .`), a mesma CLI está disponível como módulo ou script de console:

```bash
python -m pipeline --ano-filtro 2025
projeto-final --ano 2025
```

Se você abriu um terminal novo, ative de novo o venv antes:

- PowerShell: `.\.venv\Scripts\Activate.ps1` (rode antes `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force` se necessário)
- CMD: `.venv\Scripts\activate.bat`
- Git Bash: `source .venv/Scripts/activate`



### Onde fica o resultado?

O relatório Parquet é salvo em:

```
data/output/relatorio_pedidos_recusados_legitimos/
```

> O diretório é sobrescrito a cada execução (modo `overwrite`).

---

## Executar os Testes

```bash
pytest
```

Saída pensada para acompanhamento didático: cabeçalho com contexto do projeto, cada teste em linha própria (`-v`), cores quando o terminal suporta, **pytest-sugar** (barra de progresso), os 5 testes mais lentos e, ao final, um **painel resumo** (inclui aviso sobre mensagens `PID ... finalizado` no Windows após o Spark encerrar — não são falhas do pytest).

Módulos em `tests/` (um arquivo por camada, onde fizer sentido). Cada linha refere-se a **um** teste (`def test_...`): classe de produção sob verificação e foco do caso.

| Arquivo | Classe | Teste | Foco principal |
|--------|--------|-------|----------------|
| `test_pedidos_schema.py` | `PedidosSchema` | `test_pedidos_schema_get_retorna_mesma_estrutura_que_constante` | `get()` expõe o mesmo `StructType` que `SCHEMA`. |
| `test_pedidos_schema.py` | `PedidosSchema` | `test_pedidos_schema_contem_colunas_obrigatorias_do_negocio` | Tipos das colunas do CSV (id, produto, valores, datas, UF, cliente). |
| `test_pedidos_schema.py` | `PedidosSchema` | `test_pedidos_schema_id_pedido_nao_nulo` | `id_pedido` com `nullable=False` no schema explícito. |
| `test_pagamentos_schema.py` | `PagamentosSchema` | `test_pagamentos_schema_get_retorna_schema_documentado` | `get()` expõe o `StructType` principal documentado. |
| `test_pagamentos_schema.py` | `PagamentosSchema` | `test_pagamentos_schema_avaliacao_fraude_aninhada` | Struct `avaliacao_fraude` com `fraude` (bool) e `score` (double). |
| `test_pagamentos_schema.py` | `PagamentosSchema` | `test_pagamentos_schema_colunas_principais` | Tipos das colunas de primeiro nível do JSON Lines. |
| `test_pedidos_reader.py` | `PedidosReader` | `test_pedidos_reader_opcoes_csv_alinhadas_ao_dataset_real` | Opções CSV: `;`, header, UTF-8, modo PERMISSIVE. |
| `test_pedidos_reader.py` | `PedidosReader` | `test_pedidos_reader_read_parseia_data_criacao_como_timestamp` | Após `read`, `data_criacao` é `timestamp` (não string bruta). |
| `test_pedidos_reader.py` | `PedidosReader` | `test_pedidos_reader_read_preserva_todas_as_linhas_do_fixture` | Contagem de linhas do fixture gzip (nenhuma perdida pelo parser de data). |
| `test_pedidos_reader.py` | `PedidosReader` | `test_pedidos_reader_parse_data_criacao_com_sufixo_z` | Parser converte datas com sufixo 'Z' (UTC literal) sem perder o registro. |
| `test_pagamentos_reader.py` | `PagamentosReader` | `test_pagamentos_reader_opcoes_json` | `JSON_OPTIONS`: PERMISSIVE e `timestampFormat` compatível com o dataset. |
| `test_pagamentos_reader.py` | `PagamentosReader` | `test_pagamentos_reader_read_carrega_avaliacao_fraude_aninhada` | Campo `avaliacao_fraude.fraude` acessível após a leitura. |
| `test_pagamentos_reader.py` | `PagamentosReader` | `test_pagamentos_reader_read_conta_todas_as_linhas_jsonl` | Uma linha JSON por registro; contagem do fixture. |
| `test_parquet_writer.py` | `ParquetWriter` | `test_parquet_writer_construtor_guarda_modo_e_compressao` | Construtor persiste `mode` e `compression` injetados. |
| `test_parquet_writer.py` | `ParquetWriter` | `test_parquet_writer_write_encadeia_mode_opcao_compression_e_parquet` | `write` encadeia `mode` → `option(compression)` → `parquet(path)` (mock). |
| `test_relatorio_pedidos.py` | `RelatorioPedidosRecusadosLegitimos` | `test_relatorio_define_colunas_de_saida_esperadas` | `COLUNAS_SAIDA` corresponde ao contrato do relatório. |
| `test_relatorio_pedidos.py` | `RelatorioPedidosRecusadosLegitimos` | `test_construtor_armazena_ano_filtro` | Ano informado no construtor usado no filtro por ano. |
| `test_relatorio_pedidos.py` | `RelatorioPedidosRecusadosLegitimos` | `test_gerar_inclui_so_recusados_legitimos_do_ano` | Só `status=false`, `fraude=false` e ano do pedido = `ano_filtro`; colunas de saída. |
| `test_relatorio_pedidos.py` | `RelatorioPedidosRecusadosLegitimos` | `test_gerar_ordenacao_uf_forma_pagamento_data_criacao` | Ordenação `uf` → `forma_pagamento` → `data_criacao` (ASC). |
| `test_relatorio_pedidos.py` | `RelatorioPedidosRecusadosLegitimos` | `test_gerar_valor_total_e_produto_unitario_por_quantidade` | `valor_total = valor_unitario × quantidade`. |
| `test_relatorio_pedidos.py` | `RelatorioPedidosRecusadosLegitimos` | `test_gerar_data_criacao_saida_formato_string_iso` | `data_criacao` na saída como string ISO (compatível com export). |
| `test_relatorio_pedidos.py` | `RelatorioPedidosRecusadosLegitimos` | `test_gerar_outro_ano_filtro_retorna_apenas_pedidos_daquele_ano` | Trocar `ano_filtro` restringe pedidos antes do join. |
| `test_app_config.py` | `AppConfig` | `test_app_config_defaults_usam_prefixo_projeto_final` | Defaults determinísticos sem variáveis `PROJETO_FINAL_*`. |
| `test_app_config.py` | `AppConfig` | `test_app_config_le_variaveis_de_ambiente` | Sobrescrita por env (app name, ano, log, shuffle, timezone). |
| `test_app_config.py` | `AppConfig` | `test_app_config_pedidos_glob_anexa_pattern_csv_gz` | `pedidos_glob` com sufixo `pedidos-*.csv.gz`. |
| `test_app_config.py` | `AppConfig` | `test_app_config_pagamentos_glob_anexa_pattern_json_gz` | `pagamentos_glob` com sufixo `pagamentos-*.json.gz`. |
| `test_app_config.py` | `AppConfig` | `test_app_config_safe_int_rejeita_valor_nao_numerico` | Env var inválida gera `ValueError` com nome da variável. |
| `test_app_config.py` | `resolve_input_directory` | `test_resolve_input_directory_absoluto` | Caminho absoluto é apenas normalizado (`resolve`). |
| `test_app_config.py` | `resolve_input_directory` | `test_resolve_input_directory_relativo_a_raiz_do_projeto` | Relativo à raiz do repo: mesma base que `pedidos_glob` e validação pré-flight. |
| `test_app_config.py` | `resolve_project_path` | `test_resolve_project_path_relativo_a_raiz_do_projeto` | Resolver genérico ancora caminhos relativos na raiz do projeto. |
| `test_app_config.py` | `AppConfig` | `test_app_config_output_path_relativo_fica_ancorado_na_raiz` | `output_path` relativo via env é normalizado para caminho absoluto na raiz do repo. |
| `test_cli.py` | `pipeline.cli` | `test_ensure_pyspark_python_limpa_variaveis_no_windows_com_espacos` | Com espaços no caminho do Python no Windows, remove `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` para evitar falso `Missing Python executable`. |
| `test_cli.py` | `pipeline.cli` | `test_ensure_pyspark_python_define_variaveis_em_cenario_padrao` | Fora desse caso, fixa `PYSPARK_*` em `sys.executable`. |
| `test_cli.py` | `pipeline.cli` | `test_configure_stdio_utf8_windows_chama_reconfigure_no_windows` | No Windows, `stdout`/`stderr` são reconfigurados para UTF-8 antes do logging. |
| `test_cli.py` | `pipeline.cli` | `test_configure_stdio_utf8_windows_no_op_fora_do_windows` | Fora do Windows, não altera streams. |
| `test_spark_session_manager.py` | `SparkSessionManager` | `test_spark_session_manager_get_or_create_configura_builder_com_app_config` | Builder com `appName`, shuffle partitions, timezone e AQE a partir do `AppConfig` (mock). |
| `test_spark_session_manager.py` | `SparkSessionManager` | `test_spark_session_manager_get_or_create_reutiliza_instancia_interna` | Segunda chamada a `get_or_create()` reutiliza sessão em cache. |
| `test_spark_session_manager.py` | `SparkSessionManager` | `test_spark_session_manager_stop_sem_criar_sessao_nao_explode` | `stop()` sem sessão criada é no-op seguro. |
| `test_pipeline_orchestrator.py` | `PipelineOrchestrator` | `test_pipeline_orchestrator_run_chama_read_gerar_write_na_ordem` | `run`: `read` pedidos e pagamentos → `gerar` → `write` nos paths do config (mocks). |

O conjunto cobre, entre outros: filtragem `status=false` + `fraude=false`, ano configurável, `valor_total`, ordenação, colunas de saída e fluxo read → transform → write.

**Suporte (não são `test_*.py`):** `conftest.py` centraliza a `SparkSession` de teste, aplica as mesmas regras de **UTF-8 no console** (Windows) e de **`PYSPARK_*`** que `run_pipeline`, além de fixtures (`spark`, diretório gzip de pedidos/pagamentos, DataFrames de exemplo); `fixtures_datasets.py` gera os arquivos sintéticos usados pelos readers e pelo relatório.

---

<details>
<summary><strong>Configurações (variáveis de ambiente)</strong></summary>

Todas centralizadas em `src/config/app_config.py`:

| Variável                           | Padrão                                                  |
| ---------------------------------- | ------------------------------------------------------- |
| `PROJETO_FINAL_APP_NAME`           | `pedidos-recusados-legitimos`                           |
| `PROJETO_FINAL_ANO_FILTRO`         | `2025`                                                  |
| `PROJETO_FINAL_LOG_LEVEL`          | `INFO`                                                  |
| `PROJETO_FINAL_PEDIDOS_PATH`       | *(sem env: caminho **absoluto** para* `data/input/datasets-csv-pedidos/data/pedidos` *na raiz do repo)* |
| `PROJETO_FINAL_PAGAMENTOS_PATH`    | *(sem env: caminho **absoluto** para* `data/input/dataset-json-pagamentos/data/pagamentos` *na raiz do repo)* |
| `PROJETO_FINAL_OUTPUT_PATH`        | *(sem env: caminho **absoluto** para* `data/output/relatorio_pedidos_recusados_legitimos` *na raiz do repo)* |
| `PROJETO_FINAL_OUTPUT_COMPRESSION` | `snappy`                                                |
| `PROJETO_FINAL_OUTPUT_MODE`        | `overwrite`                                             |
| `PROJETO_FINAL_SHUFFLE_PARTITIONS` | `8`                                                     |
| `PROJETO_FINAL_TIMEZONE`           | `UTC`                                                   |

Caminhos relativos em `PROJETO_FINAL_PEDIDOS_PATH`, `PROJETO_FINAL_PAGAMENTOS_PATH` e `PROJETO_FINAL_OUTPUT_PATH` são resolvidos a partir da **raiz do repositório** (onde está `pyproject.toml`), evitando dependência do diretório de trabalho atual (`cwd`).

</details>

Precedência para o ano de filtro: `--ano-filtro` / `--ano` (CLI) > `PROJETO_FINAL_ANO_FILTRO` (ambiente) > valor padrão `2025`.

<details>
<summary><strong>Cobertura dos critérios da disciplina</strong></summary>

| #  | Critério                                  | Localização                                        |
| -- | ----------------------------------------- | -------------------------------------------------- |
| 1  | Schemas explícitos (sem inferência)       | `src/schemas/`                                     |
| 2  | Orientação a objetos                      | Todas as camadas                                   |
| 3  | Injeção de dependências                   | `src/pipeline/cli.py` (`run_pipeline`); entradas: `main.py` e `python -m pipeline` |
| 4  | Configurações centralizadas               | `src/config/app_config.py`                         |
| 5  | Sessão Spark                              | `src/spark/spark_session_manager.py`               |
| 6  | Leitura e escrita (I/O)                   | `src/data_io/`                                     |
| 7  | Lógica de negócio                         | `src/business/relatorio_pedidos.py`                |
| 8  | Orquestração do pipeline                  | `src/pipeline/pipeline_orchestrator.py`            |
| 9  | Logging                                   | `src/business/relatorio_pedidos.py` e `src/pipeline/cli.py` |
| 10 | Tratamento de erros                       | `try/except` + logging em `src/pipeline/cli.py` e `src/business/relatorio_pedidos.py` |
| 11 | Empacotamento                             | `pyproject.toml`, `requirements.txt`, `MANIFEST.in`|
| 12 | Testes unitários                          | `tests/` (tabela na seção **Executar os Testes**) |

</details>

<details>
<summary><strong>Solução de problemas</strong></summary>

| Problema | Solução |
| -------- | ------- |
| `JAVA_HOME is not set` | Instale o JDK e exporte `JAVA_HOME` (ver Requisitos) |
| `Permission denied` no output | Limpe `data/output/` ou altere `PROJETO_FINAL_OUTPUT_PATH` |
| Performance lenta | Reduza `PROJETO_FINAL_SHUFFLE_PARTITIONS=2` |
| Download lento do PySpark | Normal na 1ª vez (~300 MB) |
| `winutils` / `NativeIO` (Windows) | Instale `winutils.exe` (ver Requisitos) |
| `python not found` (Windows) | Desative o alias do Python em *Configurações → Aliases de execução* |
| `Activate.ps1` bloqueado (Windows) | Rode **antes** de ativar: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force` ou use CMD com `activate.bat` ou `scripts\setup_venv.cmd` |
| `ModuleNotFoundError: pyspark` com `(.venv)` ativo | O `pip install` foi feito **sem** venv ativo (instalou no Python global). Com venv ativo: `python -m pip install -e ".[dev]"` |
| `[Errno 13] Permission denied` em `.venv\Scripts\python.exe` (Git Bash) | O `.venv` já existia e o `python -m venv` tentou sobrescrever arquivos em uso. Use `bash scripts/setup_venv.sh` (reutiliza o venv) ou `deactivate`, apague `.venv` (ou `bash scripts/setup_venv.sh --recreate`) e rode de novo |
| `Missing Python executable` / `O sistema não pode encontrar o caminho` (PySpark) | Em geral: `.venv` antigo ou `PYSPARK_*` apontando para um `python.exe` inexistente. `run_pipeline` em `src/pipeline/cli.py` define `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` com o interpretador atual **exceto** no Windows quando o caminho do executável contém **espaços** (aí remove essas variáveis para o Spark não quebrar o caminho). Use sempre o mesmo Python do venv (`.\.venv\Scripts\python.exe main.py`). |
| Acentos corrompidos nos **logs** (`gera��o`, etc.) no Windows | O pipeline e o pytest reconfiguram `stdout`/`stderr` para UTF-8 antes do logging. Em CMD antigo, `chcp 65001` pode ajudar; prefira **Windows Terminal**. Mensagens `PID ... finalizado` / `ÊXITO` ao encerrar a JVM são do sistema/Java, não do pytest. |
| `[PATH_NOT_FOUND]` / glob `pedidos-*.csv.gz` | Pastas ou arquivos de entrada inexistentes: clone os datasets em `data/input` (seção **Datasets**). |
| `ensurepip not available` (Ubuntu) | `sudo apt-get install -y python3-venv` |

</details>

---

## Licença

Uso acadêmico — FIAP Data Engineering Programming.
