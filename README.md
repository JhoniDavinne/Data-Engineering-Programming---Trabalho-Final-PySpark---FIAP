# Projeto Final — Data Engineering Programming

**Disciplina:** Data Engineering Programming — FIAP  
**Professor:** Marcelo Barbosa Pinto

Pipeline PySpark que cruza dados de **pedidos** e **pagamentos** para gerar um relatório dos pedidos recusados (`status = false`) classificados como legítimos (`fraude = false`). Saída em **Parquet** (Snappy).

---

## Objetivo

A alta gestão deseja identificar pedidos cujo pagamento foi **recusado** mas a avaliação de fraude os classificou como **legítimos** (ano **2025**).

| Coluna            | Origem                                       |
| ----------------- | -------------------------------------------- |
| `id_pedido`       | `pedidos.id_pedido`                          |
| `uf`              | `pedidos.uf`                                 |
| `forma_pagamento` | `pagamentos.forma_pagamento`                 |
| `valor_total`     | `pedidos.valor_unitario × pedidos.quantidade` |
| `data_criacao`    | `pedidos.data_criacao`                       |

Ordenação: `uf` → `forma_pagamento` → `data_criacao`.

---

## Arquitetura

Orientação a objetos com injeção de dependências via `main.py` (Aggregation Root):

```
Projeto Final/
├── main.py                              # Aggregation Root (DI)
├── src/projeto_final/
│   ├── config/app_config.py             # Configurações centralizadas
│   ├── spark/spark_session_manager.py   # Sessão Spark
│   ├── schemas/                         # Schemas explícitos (pedidos + pagamentos)
│   ├── io/reader.py / writer.py         # Leitura e escrita de dados
│   ├── business/relatorio_pedidos.py    # Regra de negócio
│   └── pipeline/pipeline_orchestrator.py
├── tests/
│   ├── conftest.py                      # Fixture SparkSession
│   └── test_relatorio_pedidos.py        # Testes unitários
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

<details>
<summary><strong>Instalar Java — macOS (Homebrew)</strong></summary>

```bash
brew install --cask temurin@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

Torne persistente adicionando os `export` ao `~/.zshrc`.
</details>

<details>
<summary><strong>Instalar Java — Ubuntu/Debian</strong></summary>

```bash
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

Torne persistente adicionando os `export` ao `~/.bashrc`.
</details>

<details>
<summary><strong>Instalar Java — Fedora/RHEL</strong></summary>

```bash
sudo dnf install -y java-17-openjdk-devel
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH="$JAVA_HOME/bin:$PATH"
```
</details>

<details>
<summary><strong>Instalar Java — Windows (winget)</strong></summary>

```powershell
winget install --id Microsoft.OpenJDK.17 --silent --accept-package-agreements --accept-source-agreements
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot", "User")
$env:Path += ";$env:JAVA_HOME\bin"
```

> Ajuste o caminho do `JAVA_HOME` conforme a versão instalada em `C:\Program Files\Microsoft\`.
</details>

<details>
<summary><strong>Requisito extra Windows — <code>winutils.exe</code></strong></summary>

O Hadoop no Windows exige `winutils.exe` + `hadoop.dll` em `C:\hadoop\bin`.

```bash
mkdir -p /c/hadoop/bin
curl -L -o /c/hadoop/bin/winutils.exe \
    https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe
curl -L -o /c/hadoop/bin/hadoop.dll \
    https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/hadoop.dll
```

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
$env:Path += ";C:\hadoop\bin"
```

**Linux/macOS não precisam deste passo.**
</details>

---

## Instalação

<details>
<summary><strong>Linux / macOS</strong></summary>

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
</details>

<details>
<summary><strong>Windows (PowerShell)</strong></summary>

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```
</details>

<details>
<summary><strong>Windows (Git Bash)</strong></summary>

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
</details>

> A primeira instalação baixa o PySpark (~300 MB). Pode levar alguns minutos.

---

## Executar o Pipeline

Com o ambiente virtual ativo, na raiz do projeto:

```bash
python main.py
```

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

O arquivo `tests/test_relatorio_pedidos.py` valida:

1. Filtragem `status=false` + `fraude=false`
2. Filtragem pelo ano 2025
3. Cálculo `valor_total = valor_unitario × quantidade`
4. Ordenação (`uf`, `forma_pagamento`, `data_criacao`)
5. Schema de saída (colunas esperadas)

---

## Datasets

Os datasets estão em `data/input/`, clonados dos repositórios do professor:

| Dataset      | Repositório                                              | Caminho local                                           |
| ------------ | -------------------------------------------------------- | ------------------------------------------------------- |
| **Pedidos**  | [datasets-csv-pedidos](https://github.com/infobarbosa/datasets-csv-pedidos)   | `data/input/datasets-csv-pedidos/data/pedidos/`         |
| **Pagamentos** | [dataset-json-pagamentos](https://github.com/infobarbosa/dataset-json-pagamentos) | `data/input/dataset-json-pagamentos/data/pagamentos/`   |

---

<details>
<summary><strong>Configurações (variáveis de ambiente)</strong></summary>

Todas centralizadas em `src/projeto_final/config/app_config.py`:

| Variável                           | Padrão                                                  |
| ---------------------------------- | ------------------------------------------------------- |
| `PROJETO_FINAL_APP_NAME`           | `pedidos-recusados-legitimos`                           |
| `PROJETO_FINAL_ANO_FILTRO`         | `2025`                                                  |
| `PROJETO_FINAL_LOG_LEVEL`          | `INFO`                                                  |
| `PROJETO_FINAL_PEDIDOS_PATH`       | `data/input/datasets-csv-pedidos/data/pedidos`          |
| `PROJETO_FINAL_PAGAMENTOS_PATH`    | `data/input/dataset-json-pagamentos/data/pagamentos`    |
| `PROJETO_FINAL_OUTPUT_PATH`        | `data/output/relatorio_pedidos_recusados_legitimos`     |
| `PROJETO_FINAL_OUTPUT_COMPRESSION` | `snappy`                                                |
| `PROJETO_FINAL_OUTPUT_MODE`        | `overwrite`                                             |
| `PROJETO_FINAL_SHUFFLE_PARTITIONS` | `8`                                                     |
| `PROJETO_FINAL_TIMEZONE`           | `UTC`                                                   |

</details>

<details>
<summary><strong>Cobertura dos critérios da disciplina</strong></summary>

| #  | Critério                                  | Localização                                        |
| -- | ----------------------------------------- | -------------------------------------------------- |
| 1  | Schemas explícitos (sem inferência)       | `src/projeto_final/schemas/`                       |
| 2  | Orientação a objetos                      | Todas as camadas                                   |
| 3  | Injeção de dependências                   | `main.py`                                          |
| 4  | Configurações centralizadas               | `src/projeto_final/config/app_config.py`           |
| 5  | Sessão Spark                              | `src/projeto_final/spark/spark_session_manager.py` |
| 6  | Leitura e escrita (I/O)                   | `src/projeto_final/io/`                            |
| 7  | Lógica de negócio                         | `src/projeto_final/business/relatorio_pedidos.py`  |
| 8  | Orquestração do pipeline                  | `src/projeto_final/pipeline/pipeline_orchestrator.py` |
| 9  | Logging                                   | `relatorio_pedidos.py` e `main.py`                 |
| 10 | Tratamento de erros                       | `try/except` + logging em `main.py` e business     |
| 11 | Empacotamento                             | `pyproject.toml`, `requirements.txt`, `MANIFEST.in`|
| 12 | Testes unitários                          | `tests/test_relatorio_pedidos.py`                  |

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
| `ensurepip not available` (Ubuntu) | `sudo apt-get install -y python3-venv` |

</details>

---

## Licença

Uso acadêmico — FIAP Data Engineering Programming.
