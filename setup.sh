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

# Solicita senha do PostgreSQL
read -sp "Senha do PostgreSQL para usuário $DB_USER: " PGPASSWORD
export PGPASSWORD
echo ""

# Cria o banco de dados
echo "1. Criando banco de dados e tabelas..."
psql -U "$DB_USER" -f sql/create_database.sql
echo "✓ Banco de dados criado: $DB_NAME"

# Cria as views
echo ""
echo "2. Criando views..."
psql -U "$DB_USER" -d "$DB_NAME" -f sql/views.sql
echo "✓ Views criadas"

# Cria as procedures
echo ""
echo "3. Criando procedures..."
psql -U "$DB_USER" -d "$DB_NAME" -f sql/procedures.sql
echo "✓ Procedures criadas"

# Cria os triggers
echo ""
echo "4. Criando triggers..."
psql -U "$DB_USER" -d "$DB_NAME" -f sql/triggers.sql
echo "✓ Triggers criados"

# Importa dados
echo ""
echo "5. Importando dados do CSV..."
if [ -f "$CSV_FILE" ]; then
    read -sp "Senha do PostgreSQL novamente para ETL: " ETL_PASSWORD
    echo ""
    python3 etl/import_data.py \
        --host localhost \
        --database "$DB_NAME" \
        --user "$DB_USER" \
        --password "$ETL_PASSWORD" \
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
