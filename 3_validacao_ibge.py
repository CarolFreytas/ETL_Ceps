import logging
import time
from datetime import datetime
from pathlib import Path
import requests
import pandas as pd
from tqdm import tqdm

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

PASTA_SILVER = BASE_DIR / "silver"
PASTA_GOLD = BASE_DIR / "gold"
PASTA_ARQUIVOS = BASE_DIR / "arquivos"
PASTA_LOG = BASE_DIR / "logs"
PASTA_AUDITORIA = BASE_DIR / "auditoria"

# ==========================================================
# CRIAR PASTAS
# ==========================================================

PASTA_GOLD.mkdir(parents=True, exist_ok=True)

PASTA_LOG.mkdir(parents=True, exist_ok=True)

PASTA_AUDITORIA.mkdir(parents=True, exist_ok=True)

# ==========================================================
# ARQUIVOS AUXILIARES
# ==========================================================

ARQUIVO_IBGE = PASTA_ARQUIVOS / "cad_municipio.csv"

ARQUIVO_UF = PASTA_ARQUIVOS / "cad_uf.csv"

ARQUIVO_AUDITORIA = PASTA_AUDITORIA / "auditoria_gold.csv"

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
# VALIDAÇÕES DAS PASTAS
# ==========================================================

if not PASTA_SILVER.exists():

    raise Exception(f"Pasta Silver não encontrada:\n{PASTA_SILVER}")

if not ARQUIVO_IBGE.exists():

    raise Exception(f"Arquivo não encontrado:\n{ARQUIVO_IBGE}")

if not ARQUIVO_UF.exists():

    raise Exception(f"Arquivo não encontrado:\n{ARQUIVO_UF}")

# ==========================================================
# LISTAR ARQUIVOS SILVER
# ==========================================================

arquivos = sorted(PASTA_SILVER.glob("*.csv"))

if len(arquivos) == 0:

    raise Exception("Nenhum arquivo Silver encontrado.")

# ==========================================================
# CABEÇALHO
# ==========================================================

print()

print("=" * 70)
print("ETAPA GOLD - VALIDAÇÃO IBGE")
print("=" * 70)
print(f"Arquivos Silver encontrados: {len(arquivos)}")
print("=" * 70)

logger.info("=" * 70)
logger.info("Iniciando Etapa Gold")
logger.info(f"Arquivos Silver: {len(arquivos)}")

# ==========================================================
# FUNÇÃO PADRÃO DE LEITURA
# ==========================================================


def ler_csv(arquivo):

    df = pd.read_csv(
        arquivo, sep=None, engine="python", dtype=str, keep_default_na=False
    )

    df.columns = df.columns.str.replace('"', "", regex=False).str.strip().str.upper()

    return df


# ==========================================================
# CARREGAR BASES AUXILIARES
# ==========================================================

print()

print("Carregando bases auxiliares...")

ibge = ler_csv(ARQUIVO_IBGE)

uf_base = ler_csv(ARQUIVO_UF)

print(f"Municípios IBGE : {len(ibge):,}")

print(f"UFs cadastradas : {len(uf_base):,}")

logger.info(f"Municípios carregados: {len(ibge)}")

logger.info(f"UFs carregadas: {len(uf_base)}")

# ==========================================================
# NORMALIZAÇÃO DAS BASES
# ==========================================================

ibge.columns = ibge.columns.str.upper().str.strip()

uf_base.columns = uf_base.columns.str.upper().str.strip()

ibge = ibge.apply(lambda col: col.str.strip() if col.dtype == object else col)

uf_base = uf_base.apply(lambda col: col.str.strip() if col.dtype == object else col)

print()

print("Bases auxiliares carregadas com sucesso.")

logger.info("Bases auxiliares carregadas.")

# ==========================================================
# PROCESSAR CADA ARQUIVO SILVER
# ==========================================================

for arquivo in arquivos:

    UF = arquivo.stem.upper()

    print()
    print("=" * 70)
    print(f"PROCESSANDO UF {UF}")
    print("=" * 70)

    logger.info(f"Iniciando processamento da UF {UF}")

    # ======================================================
    # LEITURA DA SILVER
    # ======================================================

    df = ler_csv(arquivo)

    registros_silver = len(df)

    print(f"Registros Silver: {registros_silver:,}")

    logger.info(f"{UF}: {registros_silver:,} registros carregados.")

    # ======================================================
    # NORMALIZAÇÃO
    # ======================================================

    df.columns = df.columns.str.upper().str.strip()

    # CEP

    df["CEP"] = (
        df["CEP"]
        .astype(str)
        .str.replace("-", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
        .str.zfill(8)
    )

    # UF

    df["UF_API"] = df["UF_API"].astype(str).str.upper().str.strip()

    # Município

    df["MUNICIPIO_API"] = df["MUNICIPIO_API"].astype(str).str.upper().str.strip()

    # Código Município

    df["CD_MUNICIPIO"] = df["CD_MUNICIPIO"].astype(str).str.strip()

    # ======================================================
    # REMOVER DUPLICADOS
    # ======================================================

    antes = len(df)

    df.drop_duplicates(subset="CEP", inplace=True)

    duplicados = antes - len(df)

    # ======================================================
    # VALIDAR CEP
    # ======================================================

    antes = len(df)

    df = df[df["CEP"].str.match(r"^\d{8}$", na=False)]

    cep_invalidos = antes - len(df)

    # ======================================================
    # RESET INDEX
    # ======================================================

    df.reset_index(drop=True, inplace=True)

    # ======================================================
    # CONTADORES
    # ======================================================

    registros_gold = len(df)

    inicio = time.time()

    # ======================================================
    # ARQUIVO DE SAÍDA
    # ======================================================

    arquivo_saida = PASTA_GOLD / f"{UF}.csv"

    # ======================================================
    # RESUMO DA ENTRADA
    # ======================================================

    print()

    print(f"Duplicados removidos : {duplicados:,}")

    print(f"CEP válidos          : {registros_gold:,}")

    print(f"CEP inválidos        : {cep_invalidos:,}")

    logger.info(f"{UF}: " f"Duplicados={duplicados} | " f"Inválidos={cep_invalidos}")

    # ==========================================================
# MERGE COM CAD_MUNICIPIO
# ==========================================================

    logger.info(f"{UF}: iniciando Merge CAD_MUNICIPIO")

    df = df.merge(ibge, how="left", on="CD_MUNICIPIO", suffixes=("", "_IBGE"))

    logger.info(f"{UF}: Merge CAD_MUNICIPIO finalizado")

# ==========================================================
# IDENTIFICAR COLUNA DO MUNICÍPIO IBGE
# ==========================================================

    col_municipio_ibge = None

    for coluna in ["NM_MUNICIPIO_IBGE", "NM_MUNICIPIO", "NOME_MUNICIPIO", "DS_MUNICIPIO"]:

        if coluna in df.columns:

            col_municipio_ibge = coluna

            break

    if col_municipio_ibge is None:

        raise Exception("Nenhuma coluna de município encontrada na base IBGE.")

# ==========================================================
# MERGE COM CAD_UF
# ==========================================================

    logger.info(f"{UF}: iniciando Merge CAD_UF")

    if "CD_UF" not in df.columns:

        raise Exception("A coluna CD_UF não foi encontrada após o Merge do município.")

    df = df.merge(uf_base, how="left", on="CD_UF", suffixes=("", "_UF"))

    logger.info(f"{UF}: Merge CAD_UF finalizado")

# ==========================================================
# NORMALIZAÇÃO PÓS-MERGE
# ==========================================================

    df[col_municipio_ibge] = (
        df[col_municipio_ibge].fillna("").astype(str).str.upper().str.strip()
    )

    df["MUNICIPIO_API"] = df["MUNICIPIO_API"].fillna("").astype(str).str.upper().str.strip()

    df["UF_API"] = df["UF_API"].fillna("").astype(str).str.upper().str.strip()

    df["SG_UF"] = df["SG_UF"].fillna("").astype(str).str.upper().str.strip()

# ==========================================================
# STATUS IBGE
# ==========================================================

    df["STATUS_IBGE"] = "OK"

    df.loc[df[col_municipio_ibge] == "", "STATUS_IBGE"] = "IBGE_NAO_ENCONTRADO"

# ==========================================================
# STATUS UF
# ==========================================================

    df["STATUS_UF"] = "OK"

    df.loc[df["UF_API"] != df["SG_UF"], "STATUS_UF"] = "UF_DIVERGENTE"

# ==========================================================
# STATUS MUNICÍPIO
# ==========================================================

    df["STATUS_MUNICIPIO"] = "OK"

    df.loc[
        df["MUNICIPIO_API"] != df[col_municipio_ibge], "STATUS_MUNICIPIO"
    ] = "NOME_DIVERGENTE"

# ==========================================================
# STATUS FINAL (VETORIZADO)
# ==========================================================

    df["STATUS_FINAL"] = "OK"

    # prioridade 1

    df.loc[df["STATUS_API"] != "OK", "STATUS_FINAL"] = df["STATUS_API"]

    # prioridade 2

    df.loc[
        (df["STATUS_FINAL"] == "OK") & (df["STATUS_IBGE"] != "OK"), "STATUS_FINAL"
    ] = "ERRO_IBGE"

    # prioridade 3

    df.loc[
        (df["STATUS_FINAL"] == "OK") & (df["STATUS_UF"] != "OK"), "STATUS_FINAL"
    ] = "ERRO_UF"

    # prioridade 4

    df.loc[
        (df["STATUS_FINAL"] == "OK") & (df["STATUS_MUNICIPIO"] != "OK"), "STATUS_FINAL"
    ] = "AJUSTAR_NOME"

# ==========================================================
# ESTATÍSTICAS DA VALIDAÇÃO
# ==========================================================

    ibge_ok = (df["STATUS_IBGE"] == "OK").sum()

    ibge_erro = (df["STATUS_IBGE"] != "OK").sum()

    uf_ok = (df["STATUS_UF"] == "OK").sum()

    uf_erro = (df["STATUS_UF"] != "OK").sum()

    municipio_ok = (df["STATUS_MUNICIPIO"] == "OK").sum()

    municipio_erro = (df["STATUS_MUNICIPIO"] != "OK").sum()

    logger.info(
        f"{UF}: "
        f"IBGE_OK={ibge_ok} | "
        f"IBGE_ERRO={ibge_erro} | "
        f"UF_ERRO={uf_erro} | "
        f"MUNICIPIO_ERRO={municipio_erro}"
    )

    print()

    print("=" * 70)

    print("VALIDAÇÃO IBGE")

    print("=" * 70)

    print(f"IBGE OK...............: {ibge_ok:,}")

    print(f"IBGE Não Encontrado...: {ibge_erro:,}")

    print(f"UF Divergente.........: {uf_erro:,}")

    print(f"Município Divergente..: {municipio_erro:,}")

    print("=" * 70)

# ==========================================================
# EXPORTAÇÃO GOLD
# ==========================================================

    logger.info(f"{UF}: exportando camada Gold")

    df.sort_values(by="CEP", inplace=True)

    df.reset_index(drop=True, inplace=True)

    df.to_csv(arquivo_saida, sep=";", index=False, encoding="utf-8-sig")

    logger.info(f"{UF}: arquivo Gold salvo -> {arquivo_saida.name}")

# ==========================================================
# ESTATÍSTICAS FINAIS
# ==========================================================

    status_final = df["STATUS_FINAL"].value_counts().to_dict()

    ok_total = status_final.get("OK", 0)

    erro_ibge = status_final.get("ERRO_IBGE", 0)

    erro_uf = status_final.get("ERRO_UF", 0)

    ajustar_nome = status_final.get("AJUSTAR_NOME", 0)

    erro_api = (
        status_final.get("ERRO_API", 0)
        + status_final.get("INVALIDO_API", 0)
        + status_final.get("INVALIDO_FORMATO", 0)
    )

    tempo_total = round(time.time() - inicio, 2)

# ==========================================================
# AUDITORIA
# ==========================================================

    auditoria = pd.DataFrame(
        [
            {
                "DATA_PROCESSAMENTO": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "UF": UF,
                "REGISTROS_SILVER": registros_silver,
                "REGISTROS_GOLD": len(df),
                "DUPLICADOS": duplicados,
                "CEP_INVALIDOS": cep_invalidos,
                "IBGE_OK": ibge_ok,
                "IBGE_ERRO": ibge_erro,
                "UF_ERRO": uf_erro,
                "MUNICIPIO_ERRO": municipio_erro,
                "STATUS_OK": ok_total,
                "STATUS_ERRO_IBGE": erro_ibge,
                "STATUS_ERRO_UF": erro_uf,
                "STATUS_AJUSTAR_NOME": ajustar_nome,
                "STATUS_ERRO_API": erro_api,
                "TEMPO_SEGUNDOS": tempo_total,
                "ARQUIVO_GERADO": arquivo_saida.name,
            }
        ]
    )

    if ARQUIVO_AUDITORIA.exists():

        auditoria.to_csv(
            ARQUIVO_AUDITORIA,
            mode="a",
            header=False,
            index=False,
            sep=";",
            encoding="utf-8-sig",
        )

    else:

        auditoria.to_csv(ARQUIVO_AUDITORIA, index=False, sep=";", encoding="utf-8-sig")

    logger.info(f"{UF}: auditoria gravada.")

# ==========================================================
# RESUMO
# ==========================================================

    print()

    print("=" * 70)

    print(f"UF......................: {UF}")

    print(f"Registros Silver........: {registros_silver:,}")

    print(f"Registros Gold..........: {len(df):,}")

    print(f"Duplicados..............: {duplicados:,}")

    print(f"CEP inválidos...........: {cep_invalidos:,}")

    print()

    print("STATUS FINAL")

    print("-" * 70)

    for status, quantidade in df["STATUS_FINAL"].value_counts().sort_index().items():

        print(f"{status:<25} {quantidade:>12,}")

    print("-" * 70)

    print(f"Tempo...................: {tempo_total:.2f} s")

    print(f"Arquivo Gold............: {arquivo_saida.name}")

    print("=" * 70)

    logger.info(f"{UF}: processamento concluído em " f"{tempo_total:.2f}s")

# ==========================================================
# ENCERRAMENTO
# ==========================================================

print()

print("=" * 70)

print("PIPELINE GOLD FINALIZADO")

print("=" * 70)

logger.info("=" * 80)
logger.info("Pipeline Gold finalizado.")
