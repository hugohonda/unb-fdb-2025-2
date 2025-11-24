-- Criação do banco de dados
-- Usa template0 para permitir diferentes collations
CREATE DATABASE medicamentos_gov
    WITH ENCODING 'UTF8'
    LC_COLLATE='pt_BR.UTF-8'
    LC_CTYPE='pt_BR.UTF-8'
    TEMPLATE template0;
