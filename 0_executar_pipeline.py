from pathlib import Path
import subprocess
import sys
import time

BASE_DIR = Path(__file__).resolve().parent

SCRIPTS = [

    BASE_DIR / "1_preparar_csv.py",

    BASE_DIR / "2_consultar_cepify.py",

    BASE_DIR / "3_validacao_ibge.py",

    BASE_DIR / "4_relatorio.py"

]

def executar(script):

    print()
    print("=" * 70)
    print(f"Executando {script.name}")
    print("=" * 70)

    inicio = time.time()

    resultado = subprocess.run(

        [sys.executable, str(script)],

        cwd=BASE_DIR

    )

    tempo = round(
        time.time() - inicio,
        2
    )

    if resultado.returncode != 0:

        raise RuntimeError(
            f"Erro na execução de {script.name}"
        )

    print(f"Tempo: {tempo:.2f}s")

    return tempo

inicio_pipeline = time.time()

tempos = {}

for script in SCRIPTS:

    tempos[script] = executar(script)

tempo_total = round(

    time.time() - inicio_pipeline,

    2

)

#####################################################################################


import subprocess
import sys
import time
import logging
import webbrowser

from pathlib import Path
from datetime import datetime

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent
PROJETO = BASE_DIR.parent

PASTA_LOG = PROJETO / "logs"
PASTA_RELATORIOS = PROJETO / "relatorios"

PASTA_LOG.mkdir(
    parents=True,
    exist_ok=True
)

logging.basicConfig(

    filename=PASTA_LOG / "pipeline.log",

    level=logging.INFO,

    format="%(asctime)s | %(levelname)s | %(message)s"

)

logger = logging.getLogger(__name__)

# ==========================================================
# PIPELINE
# ==========================================================

PIPELINE = [

    {

        "nome": "Bronze",

        "arquivo": BASE_DIR / "1_preparar_csv.py"

    },

    {

        "nome": "Silver",

        "arquivo": BASE_DIR / "2_consultar_cepify.py"

    },

    {

        "nome": "Gold",

        "arquivo": BASE_DIR / "3_validacao_ibge.py"

    },

    {

        "nome": "Dashboard",

        "arquivo": BASE_DIR / "4_relatorio.py"

    }

]

print()

print("=" * 80)
print("PIPELINE RDP")
print("=" * 80)

logger.info("=" * 80)
logger.info("INICIANDO PIPELINE")
logger.info("=" * 80)

inicio_pipeline = time.time()

tempos = {}

# ==========================================================
# EXECUTOR
# ==========================================================

def executar(etapa, numero, total):

    nome = etapa["nome"]

    arquivo = etapa["arquivo"]

    print()

    print("=" * 80)

    print(f"[{numero}/{total}] {nome}")

    print("=" * 80)

    logger.info(f"Iniciando {nome}")

    if not arquivo.exists():

        raise FileNotFoundError(

            f"Script não encontrado:\n{arquivo}"

        )

    inicio = time.time()

    resultado = subprocess.run(

        [

            sys.executable,

            str(arquivo)

        ],

        cwd=str(BASE_DIR)

    )

    tempo = round(

        time.time() - inicio,

        2

    )

    if resultado.returncode != 0:

        logger.error(

            f"{nome} falhou."

        )

        raise RuntimeError(

            f"Erro durante {nome}"

        )

    tempos[nome] = tempo

    logger.info(

        f"{nome} concluído em {tempo:.2f}s"

    )

    print()

    print(f"✔ {nome} concluído.")

    print(f"Tempo: {tempo:.2f}s")
    
    # ==========================================================
# EXECUÇÃO
# ==========================================================

TOTAL_ETAPAS = len(PIPELINE)

for indice, etapa in enumerate(PIPELINE, start=1):

    executar(

        etapa,

        indice,

        TOTAL_ETAPAS

    )

tempo_total = round(

    time.time() - inicio_pipeline,

    2
)

logger.info(

    f"Pipeline finalizado em {tempo_total:.2f}s"

)

# ==========================================================
# ABRIR RELATÓRIO
# ==========================================================

htmls = sorted(

    PASTA_RELATORIOS.glob(

        "*.html"

    ),

    key=lambda x: x.stat().st_mtime,

    reverse=True

)

if htmls:

    ultimo = htmls[0]

    print()

    print("Abrindo Dashboard...")

    webbrowser.open(

        ultimo.resolve().as_uri()

    )

else:

    ultimo = None

# ==========================================================
# RESUMO
# ==========================================================

print()

print("=" * 80)

print("PIPELINE FINALIZADO")

print("=" * 80)

for etapa, tempo in tempos.items():

    print(

        f"✔ {etapa:<15}"

        f"{tempo:>8.2f}s"

    )

print()

print(f"Tempo Total : {tempo_total:.2f}s")

if ultimo:

    print()

    print("Dashboard:")

    print(ultimo)

print()

print("=" * 80)

logger.info("=" * 80)
logger.info("PIPELINE FINALIZADO")
logger.info("=" * 80)

