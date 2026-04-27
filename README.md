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
| `data_criacao`    | `pedidos.data_criacao`                       |

Ordenação: `uf` → `forma_pagamento` → `data_criacao`.

---

## Arquitetura

Orientação a objetos com injeção de dependências via `main.py` (Aggregation Root):

```
Projeto Final/
├── main.py                              # Aggregation Root (DI)
├── src/
│   ├── config/app_config.py             # Configurações centralizadas
│   ├── spark/spark_session_manager.py   # Sessão Spark
│   ├── schemas/                         # Schemas explícitos (pedidos + pagamentos)
│   ├── data_io/reader.py / writer.py    # Leitura e escrita de dados
│   ├── business/relatorio_pedidos.py    # Regra de negócio
│   └── pipeline/                        # Orquestração + ``python -m pipeline``
├── scripts/                             # setup_venv (CMD / PowerShell / Git Bash)
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

> ATENÇÃO: Execute o comando de instalação do ambiente virtual na **raiz do repositório** (pasta onde está `requirements.txt`).

<details>
<summary><strong>1 - Instalação automática (recomendado no Windows)</strong></summary>

### Instalação automática (recomendado no Windows)

Na **raiz do repositório** (pasta onde está `requirements.txt`):

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
python -m pip install -r requirements.txt
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
python -m pip install -r requirements.txt
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
python -m pip install -r requirements.txt
```

</details>

<details>
<summary><strong>- 2.4 - Windows — Git Bash (manual)</strong></summary>

Se **já existir** `.venv` (de outro terminal), **não** rode `python -m venv .venv` de novo no Git Bash — pode dar `Permission denied` em `python.exe`. Ative e instale:

```bash
cd "/c/caminho/para/Data-Engineering-Programming---Trabalho-Final-PySpark---FIAP"
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Só na **primeira** vez (sem pasta `.venv`):

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
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
python main.py --ano-filtro 2024
```

Também é possível usar o alias:

```bash
python main.py --ano 2024
```

Após instalar o projeto em modo editável (`pip install -e .`), a mesma CLI está disponível como módulo ou script de console:

```bash
python -m pipeline --ano-filtro 2024
projeto-final --ano 2024
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

O arquivo `tests/test_relatorio_pedidos.py` valida:

1. Filtragem `status=false` + `fraude=false`
2. Filtragem pelo ano configurado (default 2025)
3. Cálculo `valor_total = valor_unitario × quantidade`
4. Ordenação (`uf`, `forma_pagamento`, `data_criacao`)
5. Schema de saída (colunas esperadas)

---

## Datasets

Os arquivos **não** vêm no clone deste repositório: é preciso clonar os dois repositórios do professor **dentro de** `data/input/` (na raiz do projeto):

**Git Bash** (a partir da raiz do projeto):

```bash
mkdir -p data/input
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data/input/datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data/input/dataset-json-pagamentos
```

**PowerShell** (a partir da raiz do projeto) — **não** use `2>nul` aqui; isso é sintaxe de **CMD** e gera erro no PowerShell.

```powershell
New-Item -ItemType Directory -Path "data\input" -Force | Out-Null
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data\input\datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data\input\dataset-json-pagamentos
```

**CMD** (a partir da raiz do projeto):

```bat
mkdir data\input 2>nul
git clone https://github.com/infobarbosa/datasets-csv-pedidos.git data\input\datasets-csv-pedidos
git clone https://github.com/infobarbosa/dataset-json-pagamentos.git data\input\dataset-json-pagamentos
```

| Dataset      | Repositório                                              | Caminho local esperado                                   |
| ------------ | -------------------------------------------------------- | ------------------------------------------------------- |
| **Pedidos**  | [datasets-csv-pedidos](https://github.com/infobarbosa/datasets-csv-pedidos)   | `data/input/datasets-csv-pedidos/data/pedidos/`         |
| **Pagamentos** | [dataset-json-pagamentos](https://github.com/infobarbosa/dataset-json-pagamentos) | `data/input/dataset-json-pagamentos/data/pagamentos/`   |

Sem esses clones, o pipeline encerra com mensagem explícita (não dependa só do erro genérico `[PATH_NOT_FOUND]` do Spark).

---

<details>
<summary><strong>Configurações (variáveis de ambiente)</strong></summary>

Todas centralizadas em `src/config/app_config.py`:

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

Precedência para o ano de filtro: `--ano-filtro` / `--ano` (CLI) > `PROJETO_FINAL_ANO_FILTRO` (ambiente) > valor padrão `2025`.

<details>
<summary><strong>Cobertura dos critérios da disciplina</strong></summary>

| #  | Critério                                  | Localização                                        |
| -- | ----------------------------------------- | -------------------------------------------------- |
| 1  | Schemas explícitos (sem inferência)       | `src/schemas/`                                     |
| 2  | Orientação a objetos                      | Todas as camadas                                   |
| 3  | Injeção de dependências                   | `main.py`                                          |
| 4  | Configurações centralizadas               | `src/config/app_config.py`                         |
| 5  | Sessão Spark                              | `src/spark/spark_session_manager.py`               |
| 6  | Leitura e escrita (I/O)                   | `src/data_io/`                                     |
| 7  | Lógica de negócio                         | `src/business/relatorio_pedidos.py`                |
| 8  | Orquestração do pipeline                  | `src/pipeline/pipeline_orchestrator.py`            |
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
| `Activate.ps1` bloqueado (Windows) | Rode **antes** de ativar: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force` ou use CMD com `activate.bat` ou `scripts\setup_venv.cmd` |
| `ModuleNotFoundError: pyspark` com `(.venv)` ativo | O `pip install` foi feito **sem** venv ativo (instalou no Python global). Com venv ativo: `python -m pip install -r requirements.txt` |
| `[Errno 13] Permission denied` em `.venv\Scripts\python.exe` (Git Bash) | O `.venv` já existia e o `python -m venv` tentou sobrescrever arquivos em uso. Use `bash scripts/setup_venv.sh` (reutiliza o venv) ou `deactivate`, apague `.venv` (ou `bash scripts/setup_venv.sh --recreate`) e rode de novo |
| `Missing Python executable` / `O sistema não pode encontrar o caminho` (PySpark) | Variáveis `PYSPARK_*` ou um `.venv` antigo apontavam para um `python.exe` que não existe mais. O `main.py` força o interpretador atual; use `python main.py` com o mesmo Python do venv (`.\.venv\Scripts\python.exe main.py` se necessário). |
| `[PATH_NOT_FOUND]` / glob `pedidos-*.csv.gz` | Pastas ou arquivos de entrada inexistentes: clone os datasets em `data/input` (seção **Datasets**). |
| `ensurepip not available` (Ubuntu) | `sudo apt-get install -y python3-venv` |

</details>

---

## Licença

Uso acadêmico — FIAP Data Engineering Programming.
