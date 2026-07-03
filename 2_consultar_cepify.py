
import logging
import sqlite3
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests
from datetime import datetime
from tqdm import tqdm

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

PASTA_BRONZE = BASE_DIR / "bronze"
PASTA_SILVER = BASE_DIR / "silver"
PASTA_CACHE = BASE_DIR / "cache"
PASTA_LOG = BASE_DIR / "logs"
PASTA_AUDITORIA = BASE_DIR / "auditoria"

# ======================================================
# Cria as pastas
# =======================================================
PASTA_SILVER.mkdir(parents=True, exist_ok=True)
PASTA_CACHE.mkdir(parents=True, exist_ok=True)
PASTA_LOG.mkdir(parents=True, exist_ok=True)
PASTA_AUDITORIA.mkdir(parents=True, exist_ok=True)

# ==========================================================
# SQLITE
# ==========================================================

ARQUIVO_CACHE_SQLITE = PASTA_CACHE / "cep_cache.db"

# cache temporário em memória
cache_memoria = {}


# ==========================================================
# CONFIGURAÇÕES GERAIS
# ==========================================================

MAX_THREADS = 8
TIMEOUT = 10
MAX_RETRY = 3

URL_CEPIFY = "https://cepify.com.br/ws/{}/json"

ARQUIVO_AUDITORIA = PASTA_AUDITORIA / "auditoria_silver.csv"

# ==========================================================
# LOGGER
# ==========================================================

logging.basicConfig(
    filename=PASTA_LOG / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# ==========================================================
# SQLITE
# ==========================================================

ARQUIVO_CACHE_SQLITE = PASTA_CACHE / "cep_cache.db"

if ARQUIVO_CACHE_SQLITE.exists() and ARQUIVO_CACHE_SQLITE.is_dir():
    raise RuntimeError(
        f"O caminho '{ARQUIVO_CACHE_SQLITE}' é uma pasta. "
        "Remova-a para que o SQLite possa criar o arquivo do banco."
    )

print("=" * 70)
print("DIAGNÓSTICO")
print("=" * 70)
print("BASE_DIR........:", BASE_DIR)
print("PASTA_CACHE.....:", PASTA_CACHE)
print("ARQUIVO_CACHE...:", ARQUIVO_CACHE_SQLITE)
print("BASE_DIR existe.:", BASE_DIR.exists())
print("CACHE existe....:", PASTA_CACHE.exists())
print("=" * 70)

logger.info(f"Banco SQLite: {ARQUIVO_CACHE_SQLITE}")

conn = sqlite3.connect(str(ARQUIVO_CACHE_SQLITE), check_same_thread=False, timeout=30)

cursor = conn.cursor()
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA synchronous=NORMAL;")
cursor.execute("PRAGMA temp_store=MEMORY;")
cursor.execute("PRAGMA cache_size=100000;")

cursor.execute(
    """
CREATE TABLE IF NOT EXISTS CACHE_CEP(

    CEP TEXT PRIMARY KEY,

    LOGRADOURO TEXT,

    BAIRRO TEXT,

    MUNICIPIO TEXT,

    UF TEXT,

    CD_MUNICIPIO TEXT,

    STATUS TEXT,

    DATA_CONSULTA TEXT

)
"""
)

conn.commit()

# ==========================================================
# LOCK SQLITE
# ==========================================================

sqlite_lock = threading.Lock()

# ==========================================================
# SESSION HTTP
# ==========================================================

session = requests.Session()

session.headers.update({"User-Agent": "Pipeline-Enderecos/1.0"})

# ==========================================================
# LISTAR BRONZE
# ==========================================================

arquivos = sorted(PASTA_BRONZE.glob("*.csv"))

if len(arquivos) == 0:

    raise Exception(f"Nenhum arquivo encontrado em:\n{PASTA_BRONZE}")

print("=" * 70)

print("ETAPA SILVER - CONSULTA CEPIFY")

print("=" * 70)

print(f"Arquivos Bronze encontrados: {len(arquivos)}")

print("=" * 70)

logger.info("=" * 70)
logger.info("Iniciando etapa Silver")
logger.info(f"Arquivos Bronze: {len(arquivos)}")

# ==========================================================
# CONTADORES
# ==========================================================

cache_hits = 0

api_consultas = 0

api_invalidos = 0

api_erros = 0

cep_invalidos = 0

# ==========================================================
# CACHE SQLITE
# ==========================================================


def consultar_cache(cep):

    with sqlite_lock:

        cursor.execute(
            """

            SELECT

                CEP,
                LOGRADOURO,
                BAIRRO,
                MUNICIPIO,
                UF,
                CD_MUNICIPIO,
                STATUS

            FROM CACHE_CEP

            WHERE CEP = ?

            """,
            (cep,),
        )

        resultado = cursor.fetchone()

    if resultado is None:

        return None

    return {
        "CEP": resultado[0],
        "LOGRADOURO": resultado[1],
        "BAIRRO": resultado[2],
        "MUNICIPIO": resultado[3],
        "UF": resultado[4],
        "CD_MUNICIPIO": resultado[5],
        "STATUS": resultado[6],
    }


def salvar_cache(resultado):

    with sqlite_lock:

        cursor.execute(
            """

            INSERT OR REPLACE INTO CACHE_CEP(

                CEP,

                LOGRADOURO,

                BAIRRO,

                MUNICIPIO,

                UF,

                CD_MUNICIPIO,

                STATUS,

                DATA_CONSULTA

            )

            VALUES(

                ?,?,?,?,?,?,?,datetime('now')

            )

            """,
            (
                resultado["CEP"],
                resultado["LOGRADOURO"],
                resultado["BAIRRO"],
                resultado["MUNICIPIO"],
                resultado["UF"],
                resultado["CD_MUNICIPIO"],
                resultado["STATUS"],
            ),
        )

        conn.commit()


# ==========================================================
# CONTADOR DE PROGRESSO
# ==========================================================

contador = 0

contador_lock = threading.Lock()

# ===========================================================


def atualizar_progresso(UF):

    global contador

    with contador_lock:

        contador += 1

        if contador % 500 == 0:

            print(f"[{UF}] CEPs processados: {contador:,}")


# ==========================================================
# CONSULTA CEP
# ==========================================================


def buscar_cep(row):

    global cache_hits
    global api_consultas
    global api_invalidos
    global api_erros
    global cep_invalidos
    global contador

    cep = str(row["CEP"]).strip()
    uf = str(row["UF"]).strip().upper()

    atualizar_progresso(UF)

    # ------------------------------------------------------
    # Validação do CEP
    # ------------------------------------------------------

    if len(cep) != 8 or not cep.isdigit():

        cep_invalidos += 1

        logger.warning(f"CEP inválido: {cep}")

        return {
            "CEP": cep,
            "LOGRADOURO": None,
            "BAIRRO": None,
            "MUNICIPIO": None,
            "UF": uf,
            "CD_MUNICIPIO": None,
            "STATUS": "INVALIDO_FORMATO",
            "TENTATIVAS": 0,
        }

    # ------------------------------------------------------
    # CONSULTA CACHE SQLITE
    # ------------------------------------------------------

    cache = consultar_cache(cep)

    if cache is not None:

        cache_hits += 1

        logger.info(f"Cache SQLite: {cep}")

        cache["TENTATIVAS"] = 0

        return cache

    # ------------------------------------------------------
    # CONSULTA API
    # ------------------------------------------------------

    api_consultas += 1

    url = URL_CEPIFY.format(cep)

    resultado = None

    for tentativa in range(1, MAX_RETRY + 1):

        try:

            resposta = session.get(url, timeout=TIMEOUT)

            resposta.raise_for_status()

            dados = resposta.json()

            # ----------------------------------------------
            # CEP inexistente
            # ----------------------------------------------

            if not dados or dados.get("erro") or not dados.get("cep"):

                api_invalidos += 1

                resultado = {
                    "CEP": cep,
                    "LOGRADOURO": None,
                    "BAIRRO": None,
                    "MUNICIPIO": None,
                    "UF": uf,
                    "CD_MUNICIPIO": None,
                    "STATUS": "INVALIDO_API",
                    "TENTATIVAS": tentativa,
                }

                salvar_cache(resultado)

                logger.warning(f"CEP inexistente: {cep}")

                return resultado

            # ----------------------------------------------
            # CEP encontrado
            # ----------------------------------------------

            resultado = {
                "CEP": str(dados.get("cep", "")).zfill(8),
                "LOGRADOURO": dados.get("logradouro"),
                "BAIRRO": dados.get("bairro"),
                "MUNICIPIO": dados.get("municipio"),
                "UF": dados.get("uf") or uf,
                "CD_MUNICIPIO": dados.get("ibge"),
                "STATUS": "OK",
                "TENTATIVAS": tentativa,
            }

            salvar_cache(resultado)

            logger.info(f"API OK: {cep}")

            return resultado

        # --------------------------------------------------
        # Timeout
        # --------------------------------------------------

        except requests.exceptions.Timeout:

            logger.warning(f"Timeout CEP {cep} tentativa {tentativa}")

            time.sleep(1)

        # --------------------------------------------------
        # Erros HTTP
        # --------------------------------------------------

        except requests.exceptions.RequestException as erro:

            logger.warning(f"Erro HTTP {cep}: {erro}")

            time.sleep(1)

        # --------------------------------------------------
        # Outros erros
        # --------------------------------------------------

        except Exception as erro:

            logger.error(f"Erro inesperado {cep}: {erro}")

            time.sleep(1)

    # ------------------------------------------------------
    # Falha definitiva
    # ------------------------------------------------------

    api_erros += 1

    resultado = {
        "CEP": cep,
        "LOGRADOURO": None,
        "BAIRRO": None,
        "MUNICIPIO": None,
        "UF": uf,
        "CD_MUNICIPIO": None,
        "STATUS": "ERRO_API",
        "TENTATIVAS": MAX_RETRY,
    }

    salvar_cache(resultado)

    return resultado


# ==========================================================
# PROCESSAR CADA ARQUIVO BRONZE
# ==========================================================

for arquivo in arquivos:

    UF = arquivo.stem.upper()

    print()
    print("=" * 70)
    print(f"PROCESSANDO UF {UF}")
    print("=" * 70)

    logger.info(f"Iniciando processamento da UF {UF}")

    entrada = pd.read_csv(
        arquivo,
        sep=";",
        dtype=str,
        keep_default_na=False
    )

    TOTAL_CEPS = len(entrada)

    print("=" * 70)
    print(f"Registros Bronze: {TOTAL_CEPS:,}")
    print("=" * 70)

    entrada.columns = entrada.columns.str.strip().str.upper()

    entrada.drop_duplicates(
        subset="CEP",
        inplace=True
    )

    cache_hits = 0
    api_consultas = 0
    api_invalidos = 0
    api_erros = 0
    cep_invalidos = 0
    contador = 0

    logger.info("Iniciando consultas CEPify")

    inicio = time.time()

    resultado = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:

        futuros = [
            executor.submit(buscar_cep, row)
            for _, row in entrada.iterrows()
        ]

        with tqdm(
            total=len(futuros),
            desc=f"UF {UF}",
            unit=" CEP",
            colour="green",
            ncols=120
        ) as barra:

            for future in as_completed(futuros):

                resultado.append(future.result())

                barra.update(1)

                barra.set_postfix(
                    Cache=cache_hits,
                    API=api_consultas,
                    Inv=api_invalidos,
                    Err=api_erros
                )

    fim = time.time()

    tempo_total = round(fim - inicio, 2)

    logger.info(f"{UF}: consultas finalizadas.")

    df_resultado = pd.DataFrame(resultado)

    df_resultado.drop_duplicates(
        subset="CEP",
        inplace=True
    )

    arquivo_saida = PASTA_SILVER / f"{UF}.csv"

    df_resultado.to_csv(
        arquivo_saida,
        sep=";",
        index=False,
        encoding="utf-8-sig"
    )

    logger.info(f"{UF}: Silver gerada.")

    # ======================================================
    # AUDITORIA
    # ======================================================

    auditoria = pd.DataFrame([
        {
            "DATA_PROCESSAMENTO":
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

            "UF":
                UF,

            "REGISTROS_BRONZE":
                len(entrada),

            "REGISTROS_SILVER":
                len(df_resultado),

            "CONSULTAS_API":
                api_consultas,

            "CACHE_HITS":
                cache_hits,

            "CEP_INVALIDOS":
                cep_invalidos,

            "INVALIDOS_API":
                api_invalidos,

            "ERROS_API":
                api_erros,

            "TEMPO_SEGUNDOS":
                tempo_total,

            "TEMPO_MEDIO_MS":
                round(
                    (tempo_total / len(entrada)) * 1000,
                    2
                ) if len(entrada) > 0 else 0,

            "THREADS":
                MAX_THREADS,

            "ARQUIVO_GERADO":
                arquivo_saida.name
        }
    ])

    arquivo_auditoria = (
        PASTA_AUDITORIA /
        "auditoria_silver.csv"
    )

    if arquivo_auditoria.exists():

        auditoria.to_csv(
            arquivo_auditoria,
            mode="a",
            header=False,
            index=False,
            sep=";",
            encoding="utf-8-sig"
        )

    else:

        auditoria.to_csv(
            arquivo_auditoria,
            index=False,
            sep=";",
            encoding="utf-8-sig"
        )

    print()

    print("=" * 70)

    print(f"UF......................: {UF}")
    print(f"Bronze..................: {len(entrada):,}")
    print(f"Silver..................: {len(df_resultado):,}")
    print(f"Consultas API...........: {api_consultas:,}")
    print(f"Cache SQLite............: {cache_hits:,}")
    print(f"CEP inválidos...........: {cep_invalidos:,}")
    print(f"Inválidos API...........: {api_invalidos:,}")
    print(f"Erros API...............: {api_erros:,}")
    print(f"Tempo...................: {tempo_total:.2f} s")
    print(f"Arquivo Silver..........: {arquivo_saida.name}")

    print("=" * 70)

    logger.info(f"{UF}: processamento concluído.")

print()

print("=" * 70)
print("PROCESSAMENTO SILVER FINALIZADO")
print("=" * 70)

conn.close()
session.close()

logger.info("Pipeline Silver finalizado.")