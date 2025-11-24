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

### Consultas

```bash
psql -U postgres -d medicamentos_gov -f sql/consultas.sql
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

## Notas

- CSV tem 72 linhas de cabeçalho (usar `--skip 72`)
- Campos de texto usam `TEXT` para suportar valores longos
- Scripts SQL são idempotentes (drop + create)
- ETL processa com savepoints para isolamento de erros
