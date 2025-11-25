-- Views consolidadas para simplificar consultas frequentes
-- PostgreSQL
--
-- Views disponíveis:
--   - v_precos_consolidados: Preços PF e PMVG unificados por produto
--   - v_produtos_cap: Produtos com CAP e cálculo de descontos
--   - v_resumo_laboratorios: Estatísticas agregadas por laboratório
--
-- Nota: Script é idempotente (DROP IF EXISTS + CREATE)

-- Drop views primeiro
DROP VIEW IF EXISTS v_precos_consolidados CASCADE;
DROP VIEW IF EXISTS v_produtos_cap CASCADE;
DROP VIEW IF EXISTS v_resumo_laboratorios CASCADE;

-- View: Preços Consolidados por Produto
CREATE VIEW v_precos_consolidados AS
SELECT 
    p.id_produto,
    p.codigo_ggrem,
    p.nome_produto,
    p.apresentacao,
    s.nome_substancia,
    l.nome_laboratorio,
    l.cnpj,
    ct.descricao_classe,
    tp.tipo_produto,
    rp.regime_preco,
    pf.pf_sem_impostos,
    pmvg.pmvg_sem_impostos,
    CASE 
        WHEN pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos
        ELSE pf.pf_sem_impostos
    END AS preco_referencia_governo,
    p.cap,
    p.restricao_hospitalar,
    p.comercializacao_2024,
    p.data_atualizacao
FROM produtos p
INNER JOIN substancias s ON p.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN classes_terapeuticas ct ON p.id_classe = ct.id_classe
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN regimes_preco rp ON p.id_regime = rp.id_regime
LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto AND pf.id_aliquota IS NULL
LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pmvg.id_aliquota IS NULL;

-- View: Produtos com CAP aplicável (Preço Máximo de Venda ao Governo obrigatório)
CREATE VIEW v_produtos_cap AS
SELECT 
    p.id_produto,
    p.codigo_ggrem,
    p.nome_produto,
    p.apresentacao,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    pmvg.pmvg_sem_impostos AS preco_obrigatorio,
    pf.pf_sem_impostos AS preco_fabrica,
    ROUND((pf.pf_sem_impostos - pmvg.pmvg_sem_impostos) * 100.0 / NULLIF(pf.pf_sem_impostos, 0), 2) AS percentual_desconto_cap
FROM produtos p
INNER JOIN substancias s ON p.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto AND pf.id_aliquota IS NULL
INNER JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pmvg.id_aliquota IS NULL
WHERE p.cap = 'Sim';

-- View: Resumo de Preços por Laboratório
CREATE VIEW v_resumo_laboratorios AS
SELECT 
    l.id_laboratorio,
    l.nome_laboratorio,
    l.cnpj,
    COUNT(DISTINCT p.id_produto) AS total_produtos,
    COUNT(DISTINCT p.id_substancia) AS total_substancias,
    COUNT(DISTINCT CASE WHEN p.comercializacao_2024 = 'Sim' THEN p.id_produto END) AS produtos_comercializados_2024,
    COUNT(DISTINCT CASE WHEN p.cap = 'Sim' THEN p.id_produto END) AS produtos_com_cap,
    AVG(pf.pf_sem_impostos) AS preco_medio_pf,
    MIN(pf.pf_sem_impostos) AS preco_minimo_pf,
    MAX(pf.pf_sem_impostos) AS preco_maximo_pf
FROM laboratorios l
LEFT JOIN produtos p ON l.id_laboratorio = p.id_laboratorio
LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto AND pf.id_aliquota IS NULL
GROUP BY l.id_laboratorio, l.nome_laboratorio, l.cnpj;
