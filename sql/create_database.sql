-- Script de criação do banco de dados relacional para preços de medicamentos governamentais
-- Modelo normalizado seguindo os princípios do Modelo Relacional
-- PostgreSQL

-- Criação do banco de dados
CREATE DATABASE medicamentos_gov
    WITH ENCODING 'UTF8'
    LC_COLLATE='pt_BR.UTF-8'
    LC_CTYPE='pt_BR.UTF-8';

\c medicamentos_gov;

-- Tipos ENUM
CREATE TYPE tipo_restricao AS ENUM ('Sim', 'Não', 'Não especificado');
CREATE TYPE tipo_sim_nao AS ENUM ('Sim', 'Não');
CREATE TYPE tipo_preco AS ENUM ('PF', 'PMVG');

-- Tabela de Laboratórios
CREATE TABLE laboratorios (
    id_laboratorio SERIAL PRIMARY KEY,
    cnpj VARCHAR(18) NOT NULL UNIQUE,
    nome_laboratorio VARCHAR(255) NOT NULL
);

CREATE INDEX idx_cnpj ON laboratorios(cnpj);
CREATE INDEX idx_nome ON laboratorios(nome_laboratorio);

-- Tabela de Substâncias Ativas
CREATE TABLE substancias (
    id_substancia SERIAL PRIMARY KEY,
    nome_substancia VARCHAR(255) NOT NULL UNIQUE
);

CREATE INDEX idx_nome_substancia ON substancias(nome_substancia);

-- Tabela de Classes Terapêuticas
CREATE TABLE classes_terapeuticas (
    id_classe SERIAL PRIMARY KEY,
    codigo_classe VARCHAR(20) NOT NULL UNIQUE,
    descricao_classe VARCHAR(255) NOT NULL
);

CREATE INDEX idx_descricao_classe ON classes_terapeuticas(descricao_classe);

-- Tabela de Tipos de Produto
CREATE TABLE tipos_produto (
    id_tipo SERIAL PRIMARY KEY,
    tipo_produto VARCHAR(50) NOT NULL UNIQUE
);

CREATE INDEX idx_tipo_produto ON tipos_produto(tipo_produto);

-- Tabela de Regimes de Preço
CREATE TABLE regimes_preco (
    id_regime SERIAL PRIMARY KEY,
    regime_preco VARCHAR(50) NOT NULL UNIQUE
);

CREATE INDEX idx_regime_preco ON regimes_preco(regime_preco);

-- Tabela de Alíquotas ICMS
CREATE TABLE aliquotas_icms (
    id_aliquota SERIAL PRIMARY KEY,
    aliquota DECIMAL(5,2) NOT NULL UNIQUE,
    descricao VARCHAR(50) NOT NULL
);

CREATE INDEX idx_aliquota ON aliquotas_icms(aliquota);

-- Tabela Principal de Produtos/Medicamentos
CREATE TABLE produtos (
    id_produto SERIAL PRIMARY KEY,
    codigo_ggrem VARCHAR(20) NOT NULL UNIQUE,
    registro VARCHAR(20),
    ean_1 VARCHAR(20),
    ean_2 VARCHAR(20),
    ean_3 VARCHAR(20),
    nome_produto VARCHAR(255) NOT NULL,
    apresentacao TEXT NOT NULL,
    id_substancia INTEGER NOT NULL,
    id_laboratorio INTEGER NOT NULL,
    id_classe INTEGER NOT NULL,
    id_tipo INTEGER NOT NULL,
    id_regime INTEGER NOT NULL,
    restricao_hospitalar tipo_restricao DEFAULT 'Não especificado',
    cap tipo_sim_nao DEFAULT 'Não',
    confaz_87 tipo_sim_nao DEFAULT 'Não',
    icms_zero tipo_sim_nao DEFAULT 'Não',
    analise_recursal VARCHAR(50),
    lista_concessao_credito VARCHAR(255),
    comercializacao_2024 tipo_sim_nao DEFAULT 'Não',
    tarja VARCHAR(100),
    destino_comercial VARCHAR(255),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_substancia) REFERENCES substancias(id_substancia) ON DELETE RESTRICT,
    FOREIGN KEY (id_laboratorio) REFERENCES laboratorios(id_laboratorio) ON DELETE RESTRICT,
    FOREIGN KEY (id_classe) REFERENCES classes_terapeuticas(id_classe) ON DELETE RESTRICT,
    FOREIGN KEY (id_tipo) REFERENCES tipos_produto(id_tipo) ON DELETE RESTRICT,
    FOREIGN KEY (id_regime) REFERENCES regimes_preco(id_regime) ON DELETE RESTRICT
);

CREATE INDEX idx_registro_produto ON produtos(registro);
CREATE INDEX idx_produto_nome ON produtos(nome_produto);
CREATE INDEX idx_data_atualizacao ON produtos(data_atualizacao);

-- Tabela de Preços (Preço Fábrica - PF)
CREATE TABLE precos_fabrica (
    id_preco_pf SERIAL PRIMARY KEY,
    id_produto INTEGER NOT NULL,
    id_aliquota INTEGER,
    pf_sem_impostos DECIMAL(10,2),
    pf_com_impostos DECIMAL(10,2),
    data_vigencia DATE NOT NULL,
    FOREIGN KEY (id_produto) REFERENCES produtos(id_produto) ON DELETE CASCADE,
    FOREIGN KEY (id_aliquota) REFERENCES aliquotas_icms(id_aliquota) ON DELETE RESTRICT,
    UNIQUE(id_produto, id_aliquota, data_vigencia)
);

CREATE INDEX idx_produto_pf ON precos_fabrica(id_produto);
CREATE INDEX idx_aliquota_pf ON precos_fabrica(id_aliquota);
CREATE INDEX idx_vigencia_pf ON precos_fabrica(data_vigencia);

-- Tabela de Preços Máximo de Venda ao Governo (PMVG)
CREATE TABLE precos_pmvg (
    id_preco_pmvg SERIAL PRIMARY KEY,
    id_produto INTEGER NOT NULL,
    id_aliquota INTEGER,
    pmvg_sem_impostos DECIMAL(10,2),
    pmvg_com_impostos DECIMAL(10,2),
    data_vigencia DATE NOT NULL,
    FOREIGN KEY (id_produto) REFERENCES produtos(id_produto) ON DELETE CASCADE,
    FOREIGN KEY (id_aliquota) REFERENCES aliquotas_icms(id_aliquota) ON DELETE RESTRICT,
    UNIQUE(id_produto, id_aliquota, data_vigencia)
);

CREATE INDEX idx_produto_pmvg ON precos_pmvg(id_produto);
CREATE INDEX idx_aliquota_pmvg ON precos_pmvg(id_aliquota);
CREATE INDEX idx_vigencia_pmvg ON precos_pmvg(data_vigencia);

-- Tabela de Histórico de Alterações (para auditoria)
CREATE TABLE historico_precos (
    id_historico SERIAL PRIMARY KEY,
    id_produto INTEGER NOT NULL,
    tipo_preco tipo_preco NOT NULL,
    id_aliquota INTEGER,
    valor_anterior DECIMAL(10,2),
    valor_novo DECIMAL(10,2),
    data_alteracao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario_alteracao VARCHAR(100),
    FOREIGN KEY (id_produto) REFERENCES produtos(id_produto) ON DELETE CASCADE,
    FOREIGN KEY (id_aliquota) REFERENCES aliquotas_icms(id_aliquota) ON DELETE SET NULL
);

CREATE INDEX idx_produto_historico ON historico_precos(id_produto);
CREATE INDEX idx_data_historico ON historico_precos(data_alteracao);
