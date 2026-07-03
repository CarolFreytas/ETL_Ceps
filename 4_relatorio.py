import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

PASTA_AUDITORIA = BASE_DIR / "auditoria"
PASTA_RELATORIOS = BASE_DIR / "relatorios"
PASTA_LOG = BASE_DIR / "logs"

PASTA_RELATORIOS.mkdir(parents=True, exist_ok=True)

# ==========================================================
# LOGGER
# ==========================================================

logging.basicConfig(
    filename=PASTA_LOG / "pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

logger.info("=" * 70)
logger.info("GERANDO RELATÓRIO EXECUTIVO")
logger.info("=" * 70)

print("=" * 70)
print("RELATÓRIO EXECUTIVO")
print("=" * 70)

# ==========================================================
# ARQUIVOS DE AUDITORIA
# ==========================================================

ARQUIVO_BRONZE = PASTA_AUDITORIA / "auditoria_bronze.csv"

ARQUIVO_SILVER = PASTA_AUDITORIA / "auditoria_silver.csv"

ARQUIVO_GOLD = PASTA_AUDITORIA / "auditoria_gold.csv"

# ==========================================================
# VERIFICA EXISTÊNCIA
# ==========================================================

arquivos = {"Bronze": ARQUIVO_BRONZE, "Silver": ARQUIVO_SILVER, "Gold": ARQUIVO_GOLD}

for etapa, arquivo in arquivos.items():

    if not arquivo.exists():

        raise FileNotFoundError(
            f"""
Arquivo de auditoria não encontrado.

Etapa : {etapa}

Arquivo:

{arquivo}

Execute primeiro o pipeline.
"""
        )

# ==========================================================
# FUNÇÃO PADRÃO DE LEITURA
# ==========================================================


def ler_csv(arquivo: Path) -> pd.DataFrame:

    df = pd.read_csv(
        arquivo, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig"
    )

    df.columns = df.columns.str.strip().str.upper()

    return df


# ==========================================================
# LEITURA DAS AUDITORIAS
# ==========================================================

bronze = ler_csv(ARQUIVO_BRONZE)
silver = ler_csv(ARQUIVO_SILVER)
gold = ler_csv(ARQUIVO_GOLD)

print(f"Bronze : {len(bronze)} linhas")
print(f"Silver : {len(silver)} linhas")
print(f"Gold   : {len(gold)} linhas")

logger.info(f"Bronze: {len(bronze)} registros")

logger.info(f"Silver: {len(silver)} registros")

logger.info(f"Gold: {len(gold)} registros")

# ==========================================================
# PADRONIZAÇÃO DOS TIPOS
# ==========================================================


def converter_numericos(df):

    for coluna in df.columns:

        if coluna.startswith("DATA"):

            continue

        try:

            df[coluna] = pd.to_numeric(df[coluna])

        except:

            pass

    return df


bronze = converter_numericos(bronze)
silver = converter_numericos(silver)
gold = converter_numericos(gold)

# ==========================================================
# PADRONIZAÇÃO DAS UFs
# ==========================================================

for df in (bronze, silver, gold):

    if "UF" in df.columns:

        df["UF"] = df["UF"].astype(str).str.upper().str.strip()

# ==========================================================
# CONSOLIDAÇÃO
# ==========================================================

pipeline = bronze.merge(
    silver, on="UF", how="outer", suffixes=("_BRONZE", "_SILVER")
).merge(gold, on="UF", how="outer", suffixes=("", "_GOLD"))

print()
print("=" * 70)
print("Pipeline consolidado")
print("=" * 70)

print(f"UFs encontradas: {len(pipeline)}")

logger.info(f"Pipeline consolidado: {len(pipeline)} UFs")

# ==========================================================
# DATA DA EXECUÇÃO
# ==========================================================

DATA_RELATORIO = datetime.now()

DATA_FORMATADA = DATA_RELATORIO.strftime("%d/%m/%Y %H:%M:%S")

print(f"Data relatório: {DATA_FORMATADA}")

logger.info(f"Data relatório: {DATA_FORMATADA}")

print("=" * 70)
print("Parte 1A concluída.")
print("=" * 70)

# ==========================================================
# KPIs EXECUTIVOS
# ==========================================================

print()
print("=" * 70)
print("CALCULANDO KPIs")
print("=" * 70)

# ----------------------------------------------------------
# BRONZE
# ----------------------------------------------------------

TOTAL_UFS = len(pipeline)

TOTAL_ARQUIVOS = bronze["ARQUIVOS_PROCESSADOS"].sum()

TOTAL_REGISTROS_BRONZE = bronze["REGISTROS_LIDOS"].sum()

TOTAL_REGISTROS_BRONZE_FINAIS = bronze["REGISTROS_FINAIS"].sum()

TOTAL_DUPLICADOS = bronze["DUPLICADOS_REMOVIDOS"].sum()

TOTAL_INVALIDOS_BRONZE = bronze["CEP_INVALIDOS"].sum()

# ----------------------------------------------------------
# SILVER
# ----------------------------------------------------------

TOTAL_REGISTROS_SILVER = silver["REGISTROS_SILVER"].sum()

TOTAL_CACHE = silver["CACHE_HITS"].sum()

TOTAL_API = silver["CONSULTAS_API"].sum()

TOTAL_INVALIDOS_API = silver["INVALIDOS_API"].sum()

TOTAL_ERROS_API = silver["ERROS_API"].sum()

TOTAL_INVALIDOS_FORMATO = silver["CEP_INVALIDOS"].sum()

TEMPO_TOTAL_SILVER = round(silver["TEMPO_SEGUNDOS"].sum(), 2)

TEMPO_MEDIO_SILVER = round(silver["TEMPO_MEDIO_MS"].mean(), 2)

# ----------------------------------------------------------
# GOLD
# ----------------------------------------------------------

TOTAL_GOLD = len(gold)

STATUS_FINAL = gold["STATUS_FINAL"].fillna("SEM_STATUS")

STATUS_FINAL = STATUS_FINAL.value_counts()

TOTAL_OK = STATUS_FINAL.get("OK", 0)

TOTAL_ERRO_IBGE = STATUS_FINAL.get("ERRO_IBGE", 0)

TOTAL_ERRO_UF = STATUS_FINAL.get("ERRO_UF", 0)

TOTAL_AJUSTAR_NOME = STATUS_FINAL.get("AJUSTAR_NOME", 0)

TOTAL_INVALIDO_API = STATUS_FINAL.get("INVALIDO_API", 0)

TOTAL_ERRO_API = STATUS_FINAL.get("ERRO_API", 0)

# ----------------------------------------------------------
# PERCENTUAIS
# ----------------------------------------------------------

PERC_CACHE = (
    round(TOTAL_CACHE / TOTAL_REGISTROS_BRONZE * 100, 2)
    if TOTAL_REGISTROS_BRONZE
    else 0
)

PERC_API = (
    round(TOTAL_API / TOTAL_REGISTROS_BRONZE * 100, 2) if TOTAL_REGISTROS_BRONZE else 0
)

PERC_OK = round(TOTAL_OK / TOTAL_GOLD * 100, 2) if TOTAL_GOLD else 0

PERC_ERRO_IBGE = round(TOTAL_ERRO_IBGE / TOTAL_GOLD * 100, 2) if TOTAL_GOLD else 0

PERC_ERRO_UF = round(TOTAL_ERRO_UF / TOTAL_GOLD * 100, 2) if TOTAL_GOLD else 0

PERC_AJUSTAR = round(TOTAL_AJUSTAR_NOME / TOTAL_GOLD * 100, 2) if TOTAL_GOLD else 0

# ----------------------------------------------------------
# PERFORMANCE
# ----------------------------------------------------------

THREADS_UTILIZADAS = int(silver["THREADS"].max())

MEDIA_REGISTROS_UF = round(TOTAL_REGISTROS_BRONZE / TOTAL_UFS, 0)

MEDIA_API_UF = round(TOTAL_API / TOTAL_UFS, 0)

# ----------------------------------------------------------
# MAIOR UF
# ----------------------------------------------------------

MAIOR_UF = bronze.sort_values("REGISTROS_FINAIS", ascending=False).iloc[0]

UF_MAIOR = MAIOR_UF["UF"]

UF_MAIOR_TOTAL = MAIOR_UF["REGISTROS_FINAIS"]

# ----------------------------------------------------------
# MENOR UF
# ----------------------------------------------------------

MENOR_UF = bronze.sort_values("REGISTROS_FINAIS").iloc[0]

UF_MENOR = MENOR_UF["UF"]

UF_MENOR_TOTAL = MENOR_UF["REGISTROS_FINAIS"]

# ----------------------------------------------------------
# RANKING
# ----------------------------------------------------------

ranking = bronze.sort_values("REGISTROS_FINAIS", ascending=False)[
    ["UF", "REGISTROS_FINAIS"]
].reset_index(drop=True)

# ----------------------------------------------------------
# TOP 10
# ----------------------------------------------------------

TOP10 = ranking.head(10)

# ----------------------------------------------------------
# RESUMO GERAL
# ----------------------------------------------------------

KPIS = {
    "DATA_RELATORIO": DATA_FORMATADA,
    "TOTAL_UFS": TOTAL_UFS,
    "TOTAL_ARQUIVOS": int(TOTAL_ARQUIVOS),
    "TOTAL_REGISTROS_BRONZE": int(TOTAL_REGISTROS_BRONZE),
    "TOTAL_REGISTROS_SILVER": int(TOTAL_REGISTROS_SILVER),
    "TOTAL_GOLD": int(TOTAL_GOLD),
    "CACHE_HITS": int(TOTAL_CACHE),
    "CONSULTAS_API": int(TOTAL_API),
    "ERROS_API": int(TOTAL_ERROS_API),
    "INVALIDOS_API": int(TOTAL_INVALIDOS_API),
    "CEP_INVALIDOS": int(TOTAL_INVALIDOS_FORMATO),
    "TEMPO_TOTAL": TEMPO_TOTAL_SILVER,
    "TEMPO_MEDIO": TEMPO_MEDIO_SILVER,
    "PERC_CACHE": PERC_CACHE,
    "PERC_API": PERC_API,
    "PERC_OK": PERC_OK,
    "PERC_ERRO_IBGE": PERC_ERRO_IBGE,
    "PERC_ERRO_UF": PERC_ERRO_UF,
    "PERC_AJUSTAR": PERC_AJUSTAR,
    "THREADS": THREADS_UTILIZADAS,
    "UF_MAIOR": UF_MAIOR,
    "UF_MAIOR_TOTAL": int(UF_MAIOR_TOTAL),
    "UF_MENOR": UF_MENOR,
    "UF_MENOR_TOTAL": int(UF_MENOR_TOTAL),
}

logger.info("KPIs calculados com sucesso.")

# ==========================================================
# RESUMO EXECUTIVO
# ==========================================================

print()
print("=" * 70)
print("RESUMO EXECUTIVO")
print("=" * 70)

print(f"UFs........................: {TOTAL_UFS}")

print(f"Arquivos...................: {TOTAL_ARQUIVOS}")

print(f"Registros Bronze...........: {TOTAL_REGISTROS_BRONZE:,}")

print(f"Registros Silver...........: {TOTAL_REGISTROS_SILVER:,}")

print(f"Consultas API..............: {TOTAL_API:,}")

print(f"Cache......................: {TOTAL_CACHE:,}")

print(f"% Cache....................: {PERC_CACHE}%")

print(f"% API......................: {PERC_API}%")

print(f"% Sucesso..................: {PERC_OK}%")

print(f"Tempo total................: {TEMPO_TOTAL_SILVER} s")

print(f"Tempo médio................: {TEMPO_MEDIO_SILVER} ms")

print(f"Maior UF...................: {UF_MAIOR} ({UF_MAIOR_TOTAL:,})")

print(f"Menor UF...................: {UF_MENOR} ({UF_MENOR_TOTAL:,})")

print("=" * 70)

# ==========================================================
# RELATÓRIO HTML
# ==========================================================

ARQUIVO_HTML = (
    PASTA_RELATORIOS / f"relatorio_pipeline_"
    f"{DATA_RELATORIO.strftime('%Y%m%d_%H%M%S')}.html"
)

# ==========================================================
# HTML
# ==========================================================

html = f"""
<!DOCTYPE html>

<html lang="pt-BR">

<head>

<meta charset="UTF-8">

<title>Pipeline CEP</title>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

<style>

*{{

margin:0;

padding:0;

box-sizing:border-box;

font-family:Segoe UI;

}}

body{{

background:#edf2f7;

padding:30px;

}}

.container{{

max-width:1800px;

margin:auto;

}}

h1{{

font-size:34px;

color:#1e293b;

margin-bottom:5px;

}}

h2{{

font-size:22px;

margin-top:40px;

margin-bottom:20px;

color:#1e293b;

}}

.subtitulo{{

color:#64748b;

margin-bottom:30px;

}}

.cards{{

display:grid;

grid-template-columns:repeat(auto-fit,minmax(240px,1fr));

gap:20px;

margin-bottom:35px;

}}

.card{{

background:white;

padding:25px;

border-radius:15px;

box-shadow:0 8px 18px rgba(0,0,0,.08);

transition:.2s;

}}

.card:hover{{

transform:translateY(-4px);

}}

.titulo{{

font-size:15px;

color:#64748b;

}}

.valor{{

margin-top:10px;

font-size:34px;

font-weight:bold;

color:#2563eb;

}}

.percentual{{

margin-top:8px;

font-size:14px;

color:#16a34a;

}}

.bloco{{

background:white;

padding:30px;

margin-top:25px;

border-radius:15px;

box-shadow:0 8px 18px rgba(0,0,0,.08);

}}

.grid2{{

display:grid;

grid-template-columns:1fr 1fr;

gap:25px;

}}

canvas{{

width:100%;

height:420px;

}}

table{{

width:100%;

border-collapse:collapse;

margin-top:20px;

font-size:14px;

}}

th{{

background:#2563eb;

color:white;

padding:12px;

}}

td{{

padding:10px;

border-bottom:1px solid #ddd;

text-align:center;

}}

tr:nth-child(even){{

background:#f8fafc;

}}

.footer{{

margin-top:40px;

padding:20px;

text-align:center;

font-size:13px;

color:#64748b;

}}

</style>

</head>

<body>

<div class="container">

<h1>Pipeline CEP</h1>

<div class="subtitulo">

Relatório Executivo • {DATA_FORMATADA}

</div>

<div class="cards">

<div class="card">

<div class="titulo">UFs Processadas</div>

<div class="valor">{TOTAL_UFS}</div>

</div>

<div class="card">

<div class="titulo">Registros Bronze</div>

<div class="valor">{TOTAL_REGISTROS_BRONZE:,}</div>

</div>

<div class="card">

<div class="titulo">Registros Silver</div>

<div class="valor">{TOTAL_REGISTROS_SILVER:,}</div>

</div>

<div class="card">

<div class="titulo">Registros Gold</div>

<div class="valor">{TOTAL_GOLD:,}</div>

</div>

<div class="card">

<div class="titulo">Consultas API</div>

<div class="valor">{TOTAL_API:,}</div>

<div class="percentual">{PERC_API}%</div>

</div>

<div class="card">

<div class="titulo">Cache Hits</div>

<div class="valor">{TOTAL_CACHE:,}</div>

<div class="percentual">{PERC_CACHE}%</div>

</div>

<div class="card">

<div class="titulo">Sucesso</div>

<div class="valor">{PERC_OK}%</div>

</div>

<div class="card">

<div class="titulo">Tempo Total</div>

<div class="valor">{TEMPO_TOTAL_SILVER}s</div>

</div>

</div>

<h2>Gráficos</h2>

<div class="grid2">

<div class="bloco">

<canvas id="graficoStatus"></canvas>

</div>

<div class="bloco">

<canvas id="graficoCache"></canvas>

</div>

</div>

<div class="grid2">

<div class="bloco">

<canvas id="graficoUF"></canvas>

</div>

<div class="bloco">

<canvas id="graficoTempo"></canvas>

</div>

</div>

<h2>Resumo por UF</h2>

<div class="bloco">

<table>

<tr>

<th>UF</th>

<th>Bronze</th>

<th>Silver</th>

<th>API</th>

<th>Cache</th>

<th>Tempo (s)</th>

<th>Status OK (%)</th>

</tr>

"""

# ==========================================================
# TABELA POR UF
# ==========================================================

for _, linha in pipeline.iterrows():

    html += f"""
<tr>

<td>{linha.get("UF","")}</td>

<td>{int(linha.get("REGISTROS_FINAIS",0)):,}</td>

<td>{int(linha.get("REGISTROS_SILVER",0)):,}</td>

<td>{int(linha.get("CONSULTAS_API",0)):,}</td>

<td>{int(linha.get("CACHE_HITS",0)):,}</td>

<td>{linha.get("TEMPO_SEGUNDOS",0)}</td>

<td>{linha.get("PERC_OK",0)}%</td>

</tr>
"""

html += """

</table>

</div>

"""

# ==========================================================
# DADOS DOS GRÁFICOS
# ==========================================================

ufs = bronze["UF"].tolist()

registros = bronze["REGISTROS_FINAIS"].tolist()

tempos = silver["TEMPO_SEGUNDOS"].tolist()

cache = silver["CACHE_HITS"].tolist()

api = silver["CONSULTAS_API"].tolist()

status_labels = ["OK", "ERRO IBGE", "ERRO UF", "AJUSTAR", "INVÁLIDO API", "ERRO API"]

status_valores = [
    TOTAL_OK,
    TOTAL_ERRO_IBGE,
    TOTAL_ERRO_UF,
    TOTAL_AJUSTAR_NOME,
    TOTAL_INVALIDO_API,
    TOTAL_ERRO_API,
]

# ==========================================================
# CHARTJS
# ==========================================================

html += f"""

<script>

const ufs={ufs};

const registros={registros};

const tempos={tempos};

const cache={cache};

const api={api};

const statusLabels={status_labels};

const statusValores={status_valores};

new Chart(

document.getElementById('graficoStatus'),

{{

type:'doughnut',

data:{{

labels:statusLabels,

datasets:[{{

data:statusValores

}}]

}}

}}

);

new Chart(

document.getElementById('graficoCache'),

{{

type:'bar',

data:{{

labels:ufs,

datasets:[

{{

label:'Cache',

data:cache

}},

{{

label:'API',

data:api

}}

]

}}

}}

);

new Chart(

document.getElementById('graficoUF'),

{{

type:'bar',

data:{{

labels:ufs,

datasets:[{{

label:'Registros',

data:registros

}}]

}}

}}

);

new Chart(

document.getElementById('graficoTempo'),

{{

type:'line',

data:{{

labels:ufs,

datasets:[{{

label:'Tempo (s)',

data:tempos,

fill:false,

tension:.25

}}]

}}

}}

);

</script>

"""

# ==========================================================
# RODAPÉ
# ==========================================================

html += f"""

<div class="footer">

Relatório gerado automaticamente pelo Pipeline CEP

<br><br>

Data da execução:

<b>{DATA_FORMATADA}</b>

</div>

</div>

</body>

</html>

"""

# ==========================================================
# EXPORTAÇÃO
# ==========================================================

with open(ARQUIVO_HTML, "w", encoding="utf-8") as f:

    f.write(html)

print()

print("=" * 70)

print("RELATÓRIO GERADO")

print("=" * 70)

print(ARQUIVO_HTML)

print("=" * 70)

logger.info(f"Relatório HTML salvo em {ARQUIVO_HTML}")

# ==========================================================
# ABRIR NO NAVEGADOR
# ==========================================================

import webbrowser

try:

    webbrowser.open(ARQUIVO_HTML.resolve().as_uri())

    logger.info("Relatório aberto no navegador.")

except Exception as erro:

    logger.warning(f"Não foi possível abrir o navegador: {erro}")

# ==========================================================
# RETORNO PARA O PIPELINE
# ==========================================================

RELATORIO_GERADO = ARQUIVO_HTML

print()

print("=" * 80)

print("PIPELINE FINALIZADO COM SUCESSO")

print("=" * 80)

print()

print("Etapas executadas:")

print("  ✔ Bronze")

print("  ✔ Silver")

print("  ✔ Gold")

print("  ✔ Dashboard")

print()

print(f"Relatório HTML:")

print(f"   {RELATORIO_GERADO}")

print()

print("=" * 80)
