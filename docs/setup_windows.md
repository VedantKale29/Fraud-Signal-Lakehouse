# Setup on Windows (PowerShell + VS Code)

> Two commands you will NOT use here: `make` (Linux tool -- replaced by
> `.\tasks.ps1`) and `export VAR=...` (bash syntax -- PowerShell uses
> `$env:VAR = "..."`). The `Makefile` exists only for the Linux CI runners.

## 0. One-time prerequisites

| Tool | Why | Check |
|---|---|---|
| Python 3.10/3.11 (python.org, "Add to PATH" ticked) | everything | `python --version` |
| Java 17 (Temurin JDK) -- Spark 3.5 supports 8/11/17 | PySpark needs a JVM | `java -version` |
| Docker Desktop (WSL2 backend) | local Kafka + MinIO | `docker --version` |
| Git | version control | `git --version` |
| VS Code + Python extension | IDE | -- |

**Java:** after installing, if `java -version` fails:
```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-17.0.11-hotspot", "User")
# close and reopen the terminal
```

**PySpark-on-Windows (once):** download `winutils.exe` + `hadoop.dll` for
Hadoop 3.3.x into `C:\hadoop\bin`, then:
```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
```
Alternative that removes this whole class of problem: run the repo in
**WSL2 Ubuntu** (VS Code "WSL" extension) -- Makefile and bash scripts work
as-is and it matches CI exactly.

**Speed tip:** add Windows Security exclusions for the project folder and
`venv\` -- antivirus scanning Spark temp files is most of the test runtime.

## 1. Clone + venv
```powershell
git clone <your-repo-url> fraud-signal-lakehouse
cd fraud-signal-lakehouse
python -m venv venv
.\venv\Scripts\Activate.ps1
```
If blocked: `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser` then retry.

## 2. Install + test
```powershell
.\tasks.ps1 install     # requirements.txt incl. -e . editable install + pre-commit
.\tasks.ps1 test        # expect: 40 passed
```

## 3. Local dev stack (Docker Desktop running)
```powershell
.\tasks.ps1 up          # Kafka (KRaft) + MinIO, topic created
.\tasks.ps1 produce     # chaos producer -> local Kafka
.\tasks.ps1 itest       # integration gates
.\tasks.ps1 down        # stop + wipe volumes
```
MinIO console: http://localhost:9001 (admin / admin12345).

## Daily loop in VS Code
1. Open folder; select the `venv` interpreter (bottom-right).
2. New PowerShell terminal auto-activates the venv.
3. Code -> `.\tasks.ps1 test` -> commit (pre-commit runs ruff/black).

## Common Windows errors, decoded

| Error | Cause | Fix |
|---|---|---|
| `make : not recognized` | Linux tool | `.\tasks.ps1 <task>` |
| `LOG_TO_FILE=0 : not recognized` | bash env syntax | `$env:LOG_TO_FILE = "0"` first, own line |
| `running scripts is disabled` | PS policy | `Set-ExecutionPolicy RemoteSigned -Scope CurrentUser` |
| `JAVA_HOME is not set` / `Java gateway exited` | no JVM found | Temurin 17 + JAVA_HOME + reopen terminal |
| `winutils.exe` / `HADOOP_HOME` errors | Spark-on-Windows quirk | winutils note above, or WSL2 |
| `[PYTHON_VERSION_MISMATCH] worker vs driver` + `Connection reset` walls | Spark workers found a different Python on PATH | fixed in code (conftest pins PYSPARK_PYTHON); manual: `$env:PYSPARK_PYTHON = (Get-Command python).Source` |
| `Python worker failed to connect back` / `Accept timed out` | firewall/AV slowing worker spawn (Python UDFs only) | fixed in code (loader is UDF-free, Spark bound to 127.0.0.1); if it reappears, allow python.exe + java.exe in Defender Firewall |
| `docker: error during connect` | Docker Desktop not running | start it, wait for green whale |
| `\` paths breaking configs | Windows separators | use `/` in YAML/py configs |

## Debugging PySpark stack traces (learned the hard way)
Never read a `Py4JJavaError` wall top-down. Search for `Caused by:` and
`PySparkRuntimeError` -- the real cause is one line; everything above it is
the JVM narrating the corpse.
