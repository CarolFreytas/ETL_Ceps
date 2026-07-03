import logging
from pathlib import Path
from datetime import datetime
import zipfile

import pandas as pd

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

PASTA_ENTRADA = BASE_DIR / "entrada_cep_aberto"

if not PASTA_ENTRADA.exists():
    raise Exception(
        f"Pasta de entrada não encontrada:\n{PASTA_ENTRADA}"
    )

PASTA_SAIDA = BASE_DIR / "bronze"
PASTA_LOG = BASE_DIR / "logs"
PASTA_AUDITORIA = BASE_DIR / "auditoria"

PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
PASTA_LOG.mkdir(parents=True, exist_ok=True)
PASTA_AUDITORIA.mkdir(parents=True, exist_ok=True)

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
# COLUNAS DO CEP ABERTO
# ==========================================================

COLUNAS = [
    "CEP",
    "LOGRADOURO",
    "COMPLEMENTO",
    "BAIRRO",
    "COLUNA5",
    "COLUNA6",
]

# ==========================================================
# LISTAR UFs
# ==========================================================

ufs = sorted(
    [
        p
        for p in PASTA_ENTRADA.iterdir()
        if p.is_dir()
    ]
)

if len(ufs) == 0:
    raise Exception("Nenhuma UF encontrada.")

print("=" * 70)
print(f"Total de UFs encontradas: {len(ufs)}")
print("=" * 70)

# ==========================================================
# PROCESSAMENTO
# ==========================================================

for pasta_uf in ufs:

    UF = pasta_uf.name.upper().strip()

    print(f"\nProcessando UF {UF}...")

    arquivos = sorted(
        [
            arq
            for arq in pasta_uf.iterdir()
            if arq.suffix.lower() in [".csv", ".zip"]
            and not arq.name.startswith("~")
        ]
    )

    print(f"Arquivos encontrados: {len(arquivos)}")

    if len(arquivos) == 0:

        print(f"Nenhum arquivo CSV/ZIP encontrado para {UF}")

        logger.warning(
            f"{UF}: nenhum arquivo CSV/ZIP encontrado"
        )

        continue

    arquivo_saida = PASTA_SAIDA / f"{UF}.csv"

    if arquivo_saida.exists():

        print(f"⚠ Arquivo Bronze da UF {UF} já existe")

        resposta = input(
            "Deseja sobrescrever? (S/N): "
        ).strip().upper()

        if resposta != "S":

            print("UF ignorada")

            logger.info(
                f"{UF}: processamento cancelado pelo usuário"
            )

            continue

    logger.info("=" * 60)
    logger.info(f"Iniciando processamento da UF {UF}")
    logger.info(f"Arquivos encontrados: {len(arquivos)}")

    dfs = []

    # ==========================================================
    # LEITURA DOS ARQUIVOS
    # ==========================================================

    for arquivo in arquivos:

        print(f"   Lendo {arquivo.name}")

        try:

            # ------------------------------------
            # CSV
            # ------------------------------------

            if arquivo.suffix.lower() == ".csv":

                df = pd.read_csv(
                    arquivo,
                    sep=",",
                    header=None,
                    names=COLUNAS,
                    dtype=str,
                    encoding="utf-8-sig",
                    keep_default_na=False,
                )

            # ------------------------------------
            # ZIP
            # ------------------------------------

            elif arquivo.suffix.lower() == ".zip":

                with zipfile.ZipFile(arquivo) as z:

                    csvs = [
                        nome
                        for nome in z.namelist()
                        if nome.lower().endswith(".csv")
                    ]

                    if len(csvs) == 0:
                        raise Exception(
                            f"Nenhum CSV encontrado dentro de {arquivo.name}"
                        )

                    if len(csvs) > 1:

                        logger.warning(
                            f"{arquivo.name}: mais de um CSV encontrado. "
                            f"Utilizando apenas {csvs[0]}"
                        )

                    with z.open(csvs[0]) as f:

                        df = pd.read_csv(
                            f,
                            sep=",",
                            header=None,
                            names=COLUNAS,
                            dtype=str,
                            encoding="utf-8-sig",
                            keep_default_na=False,
                        )

            else:
                continue

            df = df[["CEP"]].copy()

            df["UF"] = UF

            dfs.append(df)

        except Exception as erro:

            logger.error(
                f"{arquivo.name} -> {erro}"
            )

            print(
                f"Erro ao ler {arquivo.name}: {erro}"
            )

    # ==========================================================
    # VALIDAÇÃO
    # ==========================================================

    if len(dfs) == 0:

        logger.warning(
            f"{UF}: nenhum arquivo válido encontrado"
        )

        continue

    # ==========================================================
    # CONSOLIDAÇÃO
    # ==========================================================

    df = pd.concat(
        dfs,
        ignore_index=True
    )

    registros_lidos = len(df)

    print(
        f"\nTotal registros lidos: {registros_lidos:,}"
    )

    # ==========================================================
    # LIMPEZA
    # ==========================================================

    df["CEP"] = (
        df["CEP"]
        .astype(str)
        .str.replace("-", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
        .str.zfill(8)
    )

    df["UF"] = (
        df["UF"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    antes = len(df)

    df.drop_duplicates(
        subset="CEP",
        inplace=True
    )

    duplicados = antes - len(df)

    antes = len(df)

    df = df[
        df["CEP"].str.match(
            r"^\d{8}$",
            na=False
        )
    ]

    invalidos = antes - len(df)

    df.sort_values(
        by="CEP",
        inplace=True
    )

    df.reset_index(
        drop=True,
        inplace=True
    )

    registros_finais = len(df)

    # ==========================================================
    # EXPORTAÇÃO
    # ==========================================================

    df.to_csv(
        arquivo_saida,
        sep=";",
        index=False,
        encoding="utf-8-sig"
    )

    # ==========================================================
    # AUDITORIA
    # ==========================================================

    auditoria = pd.DataFrame(
        [
            {
                "DATA_PROCESSAMENTO": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "UF": UF,
                "ARQUIVOS_PROCESSADOS": len(arquivos),
                "REGISTROS_LIDOS": registros_lidos,
                "REGISTROS_FINAIS": registros_finais,
                "DUPLICADOS_REMOVIDOS": duplicados,
                "CEP_INVALIDOS": invalidos,
                "ARQUIVO_GERADO": arquivo_saida.name,
            }
        ]
    )

    arquivo_auditoria = (
        PASTA_AUDITORIA /
        "auditoria_bronze.csv"
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

    print(f"✔ {UF}: {registros_finais:,} CEPs")

    logger.info(
        f"UF={UF} | "
        f"Lidos={registros_lidos} | "
        f"Finais={registros_finais} | "
        f"Duplicados={duplicados} | "
        f"Invalidos={invalidos}"
    )

    print("\n" + "=" * 70)
    print("PROCESSAMENTO FINALIZADO")
    print("=" * 70)

    print(f"Arquivos processados....: {len(arquivos)}")
    print(f"Registros lidos.........: {registros_lidos:,}")
    print(f"Registros finais........: {registros_finais:,}")
    print(f"Duplicados removidos....: {duplicados:,}")
    print(f"CEP inválidos...........: {invalidos:,}")
    print(f"Arquivo Bronze..........: {arquivo_saida.name}")
    print(f"Auditoria...............: {arquivo_auditoria.name}")