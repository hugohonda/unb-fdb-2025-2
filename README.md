# Sistema de Gestão de Preços de Medicamentos Governamentais

Sistema completo para importação, armazenamento e consulta de preços de medicamentos (PF e PMVG) usando PostgreSQL.

## Estrutura do Projeto

```
├── README.md                    # Este arquivo
├── setup.sh                     # Script de setup automatizado
├── requirements.txt              # Dependências Python
├── TA_PRECO_MEDICAMENTO_GOV.csv # Dados originais
├── sql/
│   ├── create_db_only.sql       # Criação do banco
│   ├── create_database.sql      # Schema completo (tabelas, tipos, índices)
│   ├── views.sql                # Views consolidadas
│   ├── procedures.sql           # Stored procedures
│   ├── triggers.sql             # Triggers de validação e auditoria
│   └── consultas.sql            # Consultas SQL complexas
└── etl/
    ├── import_data.py           # Script ETL de importação
    └── functions.py             # Funções auxiliares Python
```

## Setup Rápido

```bash
./setup.sh
```

O script solicita a senha do PostgreSQL e executa:
1. Criação do banco de dados
2. Criação de tabelas, tipos e índices
3. Criação de views
4. Criação de procedures e functions
5. Criação de triggers
6. Importação dos dados do CSV

## Requisitos

- Python 3.7+
- PostgreSQL 12+
- psycopg2-binary

## Instalação Manual

### 1. Ambiente Python

```bash
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

### 2. Banco de Dados

```bash
# Criar banco
psql -U postgres -f sql/create_db_only.sql

# Criar schema
psql -U postgres -d medicamentos_gov -f sql/create_database.sql
psql -U postgres -d medicamentos_gov -f sql/views.sql
psql -U postgres -d medicamentos_gov -f sql/procedures.sql
psql -U postgres -d medicamentos_gov -f sql/triggers.sql
```

### 3. Importar Dados

```bash
python3 etl/import_data.py \
    --host localhost \
    --database medicamentos_gov \
    --user postgres \
    --password admin \
    --csv TA_PRECO_MEDICAMENTO_GOV.csv \
    --skip 72
```

## Componentes

### Banco de Dados

**Tabelas principais:**
- `produtos` - Medicamentos com informações completas
- `laboratorios` - Fabricantes/importadores
- `substancias` - Substâncias ativas
- `classes_terapeuticas` - Classificação terapêutica
- `precos_fabrica` - Preços Fábrica (PF)
- `precos_pmvg` - Preços Máximo Venda ao Governo (PMVG)
- `historico_precos` - Auditoria de alterações

**Views:**
- `v_precos_consolidados` - PF e PMVG consolidados
- `v_produtos_cap` - Produtos com CAP e descontos
- `v_resumo_laboratorios` - Estatísticas por laboratório

### Stored Procedures

**`sp_atualizar_preco_produto`** - Atualiza preços com validações:
- Valida existência do produto
- PMVG sem CAP: não pode exceder PF
- PF: alerta variações >50%
- Registra histórico automaticamente

**`sp_buscar_produtos`** - Busca flexível com múltiplos filtros

### Triggers

**Validação:**
- `trg_validar_preco_pf` - Preços PF devem ser positivos
- `trg_validar_pmvg_vs_pf` - PMVG sem CAP não pode exceder PF

**Auditoria:**
- `trg_auditoria_preco_pf` - Registra alterações de PF
- `trg_auditoria_preco_pmvg` - Registra alterações de PMVG
- `trg_auditoria_produto` - Registra mudanças em CAP e regime

**Manutenção:**
- `trg_atualizar_data_produto` - Atualiza timestamp automaticamente

## Uso

### Conectar ao Banco de Dados

```bash
# Com senha via variável de ambiente
export PGPASSWORD=admin
psql -U postgres -d medicamentos_gov

# Ou passar senha diretamente
PGPASSWORD=admin psql -U postgres -d medicamentos_gov
```

### Navegação Básica no psql

Uma vez conectado, use comandos úteis:

```sql
-- Listar todas as tabelas
\dt

-- Listar todas as views
\dv

-- Listar todas as procedures/functions
\df

-- Descrever estrutura de uma tabela
\d produtos
\d precos_fabrica

-- Ver dados de uma tabela
SELECT * FROM produtos LIMIT 10;
SELECT COUNT(*) FROM produtos;

-- Sair do psql
\q
```

### Consultas

```bash
# Executar todas as consultas do arquivo
PGPASSWORD=admin psql -U postgres -d medicamentos_gov -f sql/consultas.sql

# Ou executar uma consulta específica diretamente
PGPASSWORD=admin psql -U postgres -d medicamentos_gov -c "SELECT COUNT(*) FROM produtos;"
```

### Consultas Interativas

```sql
-- Ver produtos com preços
SELECT p.nome_produto, pf.pf_sem_impostos, pmvg.pmvg_sem_impostos
FROM produtos p
LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto
LIMIT 10;

-- Usar views
SELECT * FROM v_precos_consolidados LIMIT 10;
SELECT * FROM v_resumo_laboratorios LIMIT 10;
```

### Procedures

```sql
-- Atualizar preço
CALL sp_atualizar_preco_produto(
    '508015901138411',  -- codigo_ggrem
    NULL,                -- id_aliquota (NULL para sem impostos)
    'PF',                -- tipo_preco
    30.00,               -- novo_valor
    'usuario',           -- usuario
    ''                   -- resultado (OUT)
);

-- Buscar produtos
SELECT * FROM sp_buscar_produtos(
    p_substancia := 'PARACETAMOL',
    p_ordenar_por := 'preco'
);
```

### Verificar Triggers

```sql
-- Listar triggers
SELECT trigger_name, event_object_table 
FROM information_schema.triggers 
WHERE trigger_schema = 'public';

-- Ver histórico
SELECT * FROM historico_precos 
ORDER BY data_alteracao DESC 
LIMIT 20;
```

## Regras de Negócio

1. **PF (Preço Fábrica)**: Preço máximo para venda a farmácias
2. **PMVG (Preço Máximo Venda ao Governo)**: 
   - Com CAP: pode ter desconto até 21.53%
   - Sem CAP: não pode exceder PF
3. **Validações automáticas**: Triggers garantem integridade
4. **Auditoria completa**: Todas as alterações são registradas

## Exemplos Rápidos

### Explorar Dados

```bash
# Conectar ao banco
PGPASSWORD=admin psql -U postgres -d medicamentos_gov

# Dentro do psql:
-- Quantos produtos temos?
SELECT COUNT(*) FROM produtos;

-- Top 10 produtos mais caros
SELECT p.nome_produto, pf.pf_sem_impostos 
FROM produtos p
JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
ORDER BY pf.pf_sem_impostos DESC
LIMIT 10;

-- Produtos por laboratório
SELECT l.nome_laboratorio, COUNT(*) as total_produtos
FROM produtos p
JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
GROUP BY l.nome_laboratorio
ORDER BY total_produtos DESC
LIMIT 10;
```

### Usar Views

```sql
-- Preços consolidados
SELECT * FROM v_precos_consolidados LIMIT 10;

-- Produtos com CAP
SELECT * FROM v_produtos_cap LIMIT 10;

-- Resumo por laboratório
SELECT * FROM v_resumo_laboratorios ORDER BY total_produtos DESC LIMIT 10;
```

### Executar Consultas Complexas

```bash
# Todas as 5 consultas do arquivo
PGPASSWORD=admin psql -U postgres -d medicamentos_gov -f sql/consultas.sql

# Salvar resultado em arquivo
PGPASSWORD=admin psql -U postgres -d medicamentos_gov -f sql/consultas.sql > resultados.txt
```

## Consultas Complexas (sql/consultas.sql)
5 consultas SQL para análise regulatória e gestão de preços de medicamentos.

### Consulta 1: Substâncias com Maior Variação de Preço entre Laboratórios
**Objetivo**: Identificar substâncias com variações extremas de preço entre laboratórios (possível abuso de preço ou necessidade de regulação).

**Lógica de Negócio**:
- Agrupa por **substância ativa** (não por laboratório) para análise de mercado
- Considera apenas substâncias comercializadas por **múltiplos laboratórios** (>1)
- Calcula variação percentual entre menor e maior preço no mercado
- Filtra tipos inválidos (`tipo_produto != '-'`)
- Ordena por maior variação primeiro (substâncias que precisam atenção regulatória)

**Saída**: Substância, quantidade de laboratórios, quantidade de produtos, tipos diferentes, preços (mín/máx/médio) e variação percentual.

**Uso**: Identificar substâncias com FLUCONAZOL (180,932% de variação) que requerem investigação regulatória.

### Consulta 2: Produto Mais Barato por Classe Terapêutica (Sem Duplicatas)
**Objetivo**: Identificar melhor opção de custo-benefício para compras governamentais, evitando duplicatas de apresentações.

**Lógica de Negócio**:
- Agrupa por **produto** (não por apresentação) para evitar duplicatas
- Para produtos com CAP: usa PMVG como preço de referência (mais barato)
- Para produtos sem CAP: usa PF como preço de referência
- Mostra quantidade de apresentações disponíveis por produto
- Ordena por classe e menor preço (melhor custo-benefício primeiro)

**Saída**: Classe terapêutica, produto, substância, laboratório, tipo, menor preço de referência, indicador CAP e quantidade de apresentações.

**Uso**: Otimizar compras governamentais selecionando produtos mais baratos em cada classe terapêutica.

### Consulta 3: Impacto Financeiro do CAP por Laboratório
**Objetivo**: Monitorar eficácia do programa CAP e contribuição de cada laboratório para economia pública.

**Lógica de Negócio**:
- Considera apenas produtos com CAP ativo e comercialização em 2024
- Calcula diferença entre PF e PMVG (economia do governo)
- Agrupa por laboratório para identificar maiores contribuidores
- Ordena por maior economia total (maior impacto financeiro primeiro)

**Saída**: Laboratório, total de produtos CAP, valores totais (PF/PMVG), economia total e desconto percentual.

**Uso**: Avaliar eficácia do programa CAP e identificar laboratórios que mais contribuem para economia pública.

### Consulta 4: Inconsistências Agrupadas por Tipo de Problema
**Objetivo**: Identificar problemas de qualidade de dados agrupados por tipo para correção prioritária.

**Lógica de Negócio**:
- **ERRO**: Produtos sem preço cadastrado (prioridade máxima - bloqueia uso)
- **ALERTA CAP**: Produtos marcados como CAP mas sem PMVG cadastrado (inconsistência de dados)
- **ALERTA PREÇO**: Preços acima de R$ 10.000 (possíveis erros ou medicamentos especiais)
- **INFO**: Dados desatualizados (última atualização há mais de 1 ano)
- Agrupa por tipo de problema e conta produtos/substâncias/laboratórios afetados
- Ordena por severidade: ERRO > ALERTA CAP > ALERTA PREÇO > INFO

**Saída**: Tipo de problema, quantidade de produtos/substâncias/laboratórios afetados, preço médio e máximo.

**Uso**: Priorizar correções de dados: focar primeiro em ERROs (sem preço), depois ALERTAs (CAP sem PMVG).

### Consulta 5: Produtos Mais Caros por Tipo (Sem Duplicatas)
**Objetivo**: Identificar outliers de preço por tipo de produto para análise de regulação.

**Lógica de Negócio**:
- Agrupa por **produto** (não por apresentação) para evitar múltiplas entradas
- Calcula estatísticas (média, mediana, mínimo, máximo) por tipo de produto
- Compara cada produto com estatísticas do seu tipo
- Classifica em: Muito Alto (≥90% do máximo), Alto/Outlier (≥2x mediana), Alto (≥150% da média), Baixo (≤110% do mínimo), Médio (resto)
- Percentual acima da média: arredondado para inteiro quando >500% (evita decimais enganosos)
- Filtra tipos inválidos (`tipo_produto != '-'`)
- Ordena por tipo e preço descendente (mais caros primeiro)

**Saída**: Produto, substância, laboratório, tipo, preço, média e mediana do tipo, percentual acima da média, classificação.

**Uso**: Identificar produtos com preços anormalmente altos para investigação regulatória (ex: produtos classificados como "Alto (outlier)").

## Notas

- CSV tem 72 linhas de cabeçalho (usar `--skip 72`)
- Campos de texto usam `TEXT` para suportar valores longos
- Scripts SQL são idempotentes (drop + create)
- ETL processa com savepoints para isolamento de erros
