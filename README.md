# ETL_Ceps

<p align="center">

# Pipeline para Tratamento, Enriquecimento, e Validação de CEPs Brasileiros.

Os arquivos do CEP Aberto não acompanham este repositório. Eles devem ser obtidos diretamente da fonte oficial e colocados na pasta **(entrada_cep_aberto)**, pasta essa que utilizei como minha camada staging - para o pouso dos dados brutos.

Fonte: https://www.cepaberto.com/downloads/new

O projeto foi desenvolvido em Python utilizando arquitetura medalhão em camadas (Bronze, Silver e Gold), processamento paralelo, cache SQLite, auditoria completa e geração automática de Dashboard HTML.

</p>

---

# Objetivo

O **ETL_Ceps** foi desenvolvido para automatizar todo o processo de preparação, enriquecimento e validação de bases de CEPs brasileiros. Já que a fonte oficial dos correios, não disponibiliza esses dados gratuitamente.

O pipeline recebe arquivos que são extraídos do site **CEP Aberto**, onde eu realizei a limpeza, padronização, consulta automática à API CEPify, validação junto às bases oficiais do IBGE e por fim foi produzido relatórios completos de auditoria e indicadores de qualidade dos dados.

---

# Principais Funcionalidades

- Extração manual de arquivos CSV e ZIP ( Cada UF possui 5 blocos de arquivos zipados)
- Processamento em arquitetura Bronze, Silver e Gold
- Limpeza e padronização de CEPs
- Remoção de duplicidades
- Processamento paralelo (ThreadPoolExecutor)
- Cache persistente utilizando SQLite
- Consulta automática à API CEPify
- Validação de municípios e UFs utilizando bases oficiais do IBGE
- Geração de auditorias em CSV
- Logs detalhados
- Dashboard HTML automático com indicadores e gráficos
- Execução automatizada do pipeline completo

---

# Arquitetura do Pipeline

```text
                CEP Aberto (Staging)
                     │
                     ▼
          1 - Bronze (Preparação)
                     │
                     ▼
      2 - Silver (Consulta CEPify)
                     │
                     ▼
      3 - Gold (Validação IBGE)
                     │
                     ▼
      4 - Dashboard HTML
```

---

# Estrutura do Projeto

```text
ETL_Ceps/

├── scripts/
│   ├── 0_executar_pipeline.py
│   ├── 1_preparar_csv.py
│   ├── 2_consultar_cepify.py
│   ├── 3_validacao_ibge.py
│   └── 4_relatorio.py
│
├── arquivos/
│   ├── cad_municipio.csv
│   └── cad_uf.csv
│
├── entrada_cep_aberto/
├── bronze/
├── silver/
├── gold/
├── cache/
├── auditoria/
├── logs/
├── relatorios/
│
├── docs/
│
├── requirements.txt
├── LICENSE
└── README.md
```
---

# Diagrama de Arquitetura 


<img width="1536" height="1024" alt="ChatGPT Image 3 de jul  de 2026, 14_51_34" src="https://github.com/user-attachments/assets/e516d998-0cc1-4628-82c2-936214061ce2" />


---

# Fluxo de Processamento

## Bronze

Responsável pela preparação dos dados.

Principais atividades:

- leitura dos arquivos CSV
- leitura de arquivos ZIP
- consolidação dos dados (Unificar os 5 arquivos de cep, em um único arquivo)
- padronização dos CEPs
- remoção de duplicidades
- eliminação de CEPs inválidos
- geração da camada Bronze

---

## Silver

Responsável pelo enriquecimento dos dados.

Principais atividades:

- consulta à API CEPify
- cache SQLite
- retry automático
- processamento paralelo
- auditoria das consultas

---

## Gold

Responsável pela validação.

Principais atividades:

- comparação com o código dos municípios do IBGE
- validação das UFs
- validação dos nomes dos municípios
- classificação final dos registros

---

## Dashboard

Responsável pela geração do relatório executivo.

São produzidos indicadores como:

- quantidade de CEPs processados
- consultas na API
- cache hits
- CEPs inválidos
- erros
- municípios divergentes
- tempo de processamento
- gráficos interativos

---

# Tecnologias Utilizadas

- Python 3.12
- Pandas
- Requests
- SQLite
- ThreadPoolExecutor
- HTML5
- CSS3
- Chart.js
- Logging
- Pathlib
- ZipFile

---

# Bibliotecas

Instalação:

```bash
pip install -r requirements.txt
```

requirements.txt

```text
pandas
requests
tqdm
```

---

# Como Executar

Execute apenas:

```bash
python scripts/0_executar_pipeline.py
```

O pipeline executará automaticamente:

```
1_preparar_csv.py

↓

2_consultar_cepify.py

↓

3_validacao_ibge.py

↓

4_relatorio.py
```

Ao final será aberto automaticamente o Dashboard HTML.

---

# Estrutura das Camadas

| Camada | Objetivo |
|---------|----------|
| Staging | Pouso dos dados bruto |
| Bronze | Preparação dos dados |
| Silver | Enriquecimento via API |
| Gold | Validação com IBGE |
| Dashboard | Indicadores e relatórios |

---

# Auditoria

O projeto gera automaticamente auditorias contendo:

- registros processados
- CEPs válidos
- CEPs inválidos
- duplicidades
- consultas realizadas
- utilização do cache
- tempo de processamento
- arquivos gerados

---

# Logs

Todas as execuções são registradas em:

```
logs/pipeline.log
```

---

# Dashboard

O pipeline gera automaticamente um Dashboard HTML contendo:

- KPIs
- gráficos
- indicadores
- estatísticas
- resumo da execução

---

# Performance

O pipeline foi otimizado para grandes volumes de dados utilizando:

- processamento paralelo
- cache SQLite
- reutilização de consultas
- redução de chamadas à API
- leitura otimizada de arquivos

---

# Documentação

O projeto possui documentação técnica completa contendo:

- arquitetura
- regras de negócio
- funcionamento interno
- auditoria
- segurança
- performance
- manutenção

---

# Licença

Este projeto utiliza a licença MIT.

---

## 🤝 Sobre este projeto e o seu desenvolvimento com IA

Este projeto foi desenvolvido por mim com apoio da Inteligência Artificial (OpenAI ChatGPT), utilizada como assistente técnico ao longo de todo o processo.

A IA contribuiu com discussões sobre arquitetura, otimização de código, identificação e resolução de problemas, boas práticas de Engenharia de Dados, geração de documentação técnica e propostas de melhorias. Todas as implementações foram analisadas, adaptadas, testadas e validadas por mim antes de serem incorporadas ao projeto.

Este trabalho demonstra como a colaboração entre profissionais e Inteligência Artificial pode acelerar o desenvolvimento de soluções robustas, bem documentadas e alinhadas às boas práticas da Engenharia de Dados.

Este projeto demonstra como profissionais e Inteligência Artificial podem colaborar para desenvolver soluções de Engenharia de Dados mais robustas, eficientes e bem documentadas.

---

#  👩‍💻 Autor

**Carol Freytas**

Curiosa e aspirante à Engenharia de Dados
LinkedIn: https://br.linkedin.com/in/carol-freitas-107555213

- Engenharia de Dados
- Qualidade de Dados
- Governança de Dados
- Python
- SQL
- Snowflake
- Data Contracts
---

⭐⭐⭐⭐⭐ 
Caso este projeto tenha sido útil, considere deixar uma estrela no repositório.
E qualquer contribuição será muito bem vinda!
⭐⭐⭐⭐⭐
