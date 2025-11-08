-- Views para simplificar consultas frequentes
-- PostgreSQL

-- View: Preços Consolidados por Produto e Alíquota
CREATE OR REPLACE VIEW v_precos_consolidados AS
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
    a.aliquota,
    a.descricao AS descricao_aliquota,
    pf.pf_sem_impostos,
    pf.pf_com_impostos,
    pmvg.pmvg_sem_impostos,
    pmvg.pmvg_com_impostos,
    CASE 
        WHEN pmvg.pmvg_com_impostos IS NOT NULL THEN pmvg.pmvg_com_impostos
        ELSE pf.pf_com_impostos
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
LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pf.id_aliquota = pmvg.id_aliquota
LEFT JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota;

-- View: Produtos com CAP aplicável (Preço Máximo de Venda ao Governo obrigatório)
CREATE OR REPLACE VIEW v_produtos_cap AS
SELECT 
    p.id_produto,
    p.codigo_ggrem,
    p.nome_produto,
    p.apresentacao,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    a.aliquota,
    pmvg.pmvg_com_impostos AS preco_obrigatorio,
    pf.pf_com_impostos AS preco_fabrica,
    ROUND((pf.pf_com_impostos - pmvg.pmvg_com_impostos) * 100.0 / NULLIF(pf.pf_com_impostos, 0), 2) AS percentual_desconto_cap
FROM produtos p
INNER JOIN substancias s ON p.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
INNER JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pf.id_aliquota = pmvg.id_aliquota
INNER JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
WHERE p.cap = 'Sim'
ORDER BY p.nome_produto, a.aliquota;

-- View: Resumo de Preços por Laboratório
CREATE OR REPLACE VIEW v_resumo_laboratorios AS
SELECT 
    l.id_laboratorio,
    l.nome_laboratorio,
    l.cnpj,
    COUNT(DISTINCT p.id_produto) AS total_produtos,
    COUNT(DISTINCT p.id_substancia) AS total_substancias,
    COUNT(DISTINCT CASE WHEN p.comercializacao_2024 = 'Sim' THEN p.id_produto END) AS produtos_comercializados_2024,
    COUNT(DISTINCT CASE WHEN p.cap = 'Sim' THEN p.id_produto END) AS produtos_com_cap,
    AVG(pf.pf_com_impostos) AS preco_medio_pf,
    MIN(pf.pf_com_impostos) AS preco_minimo_pf,
    MAX(pf.pf_com_impostos) AS preco_maximo_pf
FROM laboratorios l
LEFT JOIN produtos p ON l.id_laboratorio = p.id_laboratorio
LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
GROUP BY l.id_laboratorio, l.nome_laboratorio, l.cnpj;
