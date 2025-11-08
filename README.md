Os dados TA_PRECO_MEDICAMENTO_GOV.csv representam a lista de preços de Medicamentos , contemplando o preço Fábrica, ou preço fabricante (PF), que é o preço máximo praticado que pode ser praticado pelas empresas produtoras ou importadoras do produto e pelas empresas distribuidoras. O PF indica o preço máximo permitido para venda a farmácias e drogarias e o Preço Máximo de Venda ao Governo (PMVG) indica o preço teto de venda aos entes da administração pública quando for aplicável o desconto do Coeficiente de Adequação de Preços (CAP), quando não for o preço teto é o PF.


## Estrutura do Projeto

```
databanks/
├── README.md                    # Este arquivo
├── INTRODUCAO.md                # Introdução explicando o problema
├── TA_PRECO_MEDICAMENTO_GOV.csv # Dados originais
├── requirements.txt              # Dependências Python
├── setup.sh                     # Script de setup automatizado
├── sql/
│   ├── create_database.sql      # Script de criação do banco PostgreSQL
│   ├── views.sql                # Views do banco de dados
│   ├── procedures.sql           # Stored procedures com comandos condicionais
│   ├── triggers.sql             # Triggers com comandos condicionais
│   ├── consultas.sql            # 5 consultas SQL complexas
│   └── algebra_relacional.md    # 3 consultas em Álgebra Relacional
└── etl/
    └── import_data.py           # Script ETL Python para importação
```

## Requisitos

- Python 3.7+
- PostgreSQL 12+
- psycopg2-binary (instalado via requirements.txt)

## Instalação

1. Instalar dependências Python:
```bash
pip install -r requirements.txt
```

2. Criar o banco de dados PostgreSQL:
```bash
# Como usuário postgres ou com permissões adequadas
psql -U postgres -f sql/create_database.sql
```

3. Criar views, procedures e triggers:
```bash
psql -U postgres -d medicamentos_gov -f sql/views.sql
psql -U postgres -d medicamentos_gov -f sql/procedures.sql
psql -U postgres -d medicamentos_gov -f sql/triggers.sql
```

## Execução do ETL

Importar dados do CSV para o banco de dados PostgreSQL:

```bash
python etl/import_data.py \
    --host localhost \
    --database medicamentos_gov \
    --user postgres \
    --password sua_senha \
    --csv TA_PRECO_MEDICAMENTO_GOV.csv \
    --skip 72
```

## Componentes Implementados

### ✅ Introdução
- **INTRODUCAO.md**: Explicação completa do problema e objetivos da solução

### ✅ Banco de Dados Relacional (PostgreSQL)
- **sql/create_database.sql**: Schema normalizado com:
  - Tipos ENUM para validação de dados
  - Tabelas principais: produtos, laboratorios, substancias, classes_terapeuticas
  - Tabelas de preços: precos_fabrica, precos_pmvg
  - Tabela de histórico: historico_precos
  - Integridade referencial com foreign keys
  - Índices para performance

### ✅ ETL em Python
- **etl/import_data.py**: Script completo de Extração, Transformação e Carga:
  - Extração do arquivo CSV
  - Normalização de dados
  - Transformação de formatos (vírgula para ponto decimal)
  - Inserção no banco relacional PostgreSQL
  - Tratamento de erros e validações

### ✅ Views
- **v_precos_consolidados**: Consolida PF e PMVG em uma única estrutura
- **v_produtos_cap**: Produtos com CAP aplicável e cálculo de desconto
- **v_resumo_laboratorios**: Resumo estatístico por laboratório

### ✅ Stored Procedures (PostgreSQL)
- **sp_atualizar_preco_produto**: Atualiza preços com validações condicionais:
  - Valida se produto existe
  - Para PMVG sem CAP: valida que não excede PF
  - Para PF: alerta variações grandes (>50%)
  - Registra histórico automaticamente
- **sp_buscar_produtos**: Busca flexível com múltiplos filtros condicionais (retorna TABLE)

### ✅ Triggers com Condicionais
- **trg_validar_preco_pf**: Valida preços PF antes de inserir/atualizar (deve ser > 0)
- **trg_validar_pmvg_vs_pf**: Valida PMVG contra PF (aplica desconto CAP quando necessário)
- **trg_auditoria_preco_pf/pmvg**: Registra alterações de preços no histórico
- **trg_auditoria_produto**: Registra alterações importantes nos produtos
- **trg_atualizar_data_produto**: Atualiza data automaticamente

### ✅ 5 Consultas SQL Complexas
1. Análise comparativa de preços entre laboratórios por substância
2. Identificação de produtos com melhor custo-benefício por classe terapêutica (com ROW_NUMBER)
3. Análise de impacto financeiro do CAP por laboratório
4. Detecção de inconsistências e produtos que requerem atenção
5. Ranking de produtos mais caros por tipo com análise comparativa (com CTEs)

### ✅ 3 Consultas em Álgebra Relacional
1. Produtos com CAP aplicável e seus preços (σ, ⋈, π)
2. Laboratórios com maior número de produtos por classe (⋈, γ, COUNT)
3. Produtos com preço acima da média da classe (γ, AVG, σ, ⋈)

## Executar Consultas

As consultas podem ser executadas diretamente no PostgreSQL:

```bash
psql -U postgres -d medicamentos_gov -f sql/consultas.sql
```

Ou interativamente:

```bash
psql -U postgres -d medicamentos_gov
medicamentos_gov=# SELECT * FROM v_precos_consolidados LIMIT 10;
```

## Usar Stored Procedures

```sql
-- Atualizar preço via procedure
CALL sp_atualizar_preco_produto(
    '538912020009303',  -- codigo_ggrem
    1,                  -- id_aliquota
    'PF',               -- tipo_preco
    150.00,             -- novo_valor
    'usuario_teste',    -- usuario
    ''                  -- resultado (OUT)
);

-- Buscar produtos via function
SELECT * FROM sp_buscar_produtos(
    p_substancia := 'PARACETAMOL',
    p_ordenar_por := 'preco'
);
```

## Vantagens do PostgreSQL

- **Procedures Nativas**: Suporte completo a stored procedures com lógica condicional
- **Tipos ENUM**: Validação de dados no nível do banco
- **Performance**: Otimizado para grandes volumes de dados
- **Recursos Avançados**: CTEs, window functions, triggers robustos
- **Concorrência**: Excelente suporte a múltiplos usuários simultâneos

## Notas

- O arquivo CSV tem 72 linhas de cabeçalho antes dos dados
- O script ETL processa em lotes de 100 linhas para melhor performance
- Triggers garantem integridade dos dados conforme regras de negócio
- Procedures podem ser chamadas diretamente via SQL ou integradas em aplicações
- Certifique-se de ter PostgreSQL instalado e rodando antes de executar
