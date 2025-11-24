#!/bin/bash
# Script para criar e popular o banco de dados PostgreSQL

set -e

DB_NAME="medicamentos_gov"
DB_USER="${POSTGRES_USER:-postgres}"
CSV_FILE="TA_PRECO_MEDICAMENTO_GOV.csv"

echo "=== Setup do Banco de Dados de Medicamentos ==="
echo ""

# Verifica se PostgreSQL está disponível
if ! command -v psql &> /dev/null; then
    echo "✗ PostgreSQL não encontrado. Instale PostgreSQL primeiro."
    exit 1
fi

# Verifica se Python está disponível
if ! command -v python3 &> /dev/null; then
    echo "✗ Python 3 não encontrado. Instale Python 3 primeiro."
    exit 1
fi

# Verifica se psycopg2 está instalado
if ! python3 -c "import psycopg2" 2>/dev/null; then
    echo "⚠ psycopg2 não encontrado."
    echo ""
    echo "Recomendado: Use um ambiente virtual Python"
    echo "  python3 -m venv .venv"
    echo "  source .venv/bin/activate"
    echo "  pip install -r requirements.txt"
    echo ""
    read -p "Deseja instalar psycopg2 agora? (s/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Ss]$ ]]; then
        pip3 install -r requirements.txt
    else
        echo "✗ Instale psycopg2 antes de continuar: pip install -r requirements.txt"
        exit 1
    fi
fi

# Solicita senha do PostgreSQL
read -sp "Senha do PostgreSQL para usuário $DB_USER: " PGPASSWORD
export PGPASSWORD
echo ""

# Cria o banco de dados
echo "1. Criando banco de dados..."
psql -U "$DB_USER" -d postgres -f sql/create_db_only.sql 2>&1 | grep -v "already exists" || true
echo "✓ Banco de dados criado: $DB_NAME"

# Cria tabelas e estruturas
echo ""
echo "2. Criando tabelas e estruturas..."
psql -U "$DB_USER" -d "$DB_NAME" -f sql/create_database.sql 2>&1 | grep -vE "(does not exist, skipping|^NOTICE:)"
echo "✓ Tabelas criadas"

# Cria as views
echo ""
echo "3. Criando views..."
psql -U "$DB_USER" -d "$DB_NAME" -f sql/views.sql 2>&1 | grep -vE "(does not exist, skipping|^NOTICE:)"
echo "✓ Views criadas"

# Cria as procedures
echo ""
echo "4. Criando procedures..."
psql -U "$DB_USER" -d "$DB_NAME" -f sql/procedures.sql 2>&1 | grep -vE "(does not exist, skipping|^NOTICE:)"
echo "✓ Procedures criadas"

# Cria os triggers
echo ""
echo "5. Criando triggers..."
psql -U "$DB_USER" -d "$DB_NAME" -f sql/triggers.sql 2>&1 | grep -vE "(does not exist, skipping|^NOTICE:)"
echo "✓ Triggers criados"

# Importa dados
echo ""
echo "6. Importando dados do CSV..."
if [ -f "$CSV_FILE" ]; then
    python3 etl/import_data.py \
        --host localhost \
        --database "$DB_NAME" \
        --user "$DB_USER" \
        --password "$PGPASSWORD" \
        --csv "$CSV_FILE" \
        --skip 72
    echo "✓ Dados importados"
else
    echo "⚠ Arquivo CSV não encontrado: $CSV_FILE"
    echo "  Execute manualmente:"
    echo "  python3 etl/import_data.py --host localhost --database $DB_NAME --user $DB_USER --password SENHA --csv $CSV_FILE"
fi

echo ""
echo "=== Setup concluído! ==="
echo ""
echo "Banco de dados disponível: $DB_NAME"
echo ""
echo "Para usar interativamente:"
echo "  psql -U $DB_USER -d $DB_NAME"
echo ""
echo "Para executar as consultas:"
echo "  psql -U $DB_USER -d $DB_NAME -f sql/consultas.sql"
