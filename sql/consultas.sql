-- 5 Consultas SQL Complexas
-- PostgreSQL

-- Consulta 1: Análise comparativa de preços entre laboratórios para a mesma substância
SELECT 
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    COUNT(DISTINCT p.id_produto) AS qtd_apresentacoes,
    AVG(pf.pf_com_impostos) AS preco_medio_pf,
    MIN(pf.pf_com_impostos) AS preco_minimo_pf,
    MAX(pf.pf_com_impostos) AS preco_maximo_pf,
    STDDEV(pf.pf_com_impostos) AS desvio_padrao_preco,
    ROUND((MAX(pf.pf_com_impostos) - MIN(pf.pf_com_impostos)) * 100.0 / NULLIF(MIN(pf.pf_com_impostos), 0), 2) AS variacao_percentual
FROM substancias s
INNER JOIN produtos p ON s.id_substancia = p.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
INNER JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
WHERE a.aliquota = 18.0
    AND p.comercializacao_2024 = 'Sim'
GROUP BY s.nome_substancia, l.nome_laboratorio, tp.tipo_produto
HAVING COUNT(DISTINCT p.id_produto) >= 1
ORDER BY s.nome_substancia, variacao_percentual DESC;

-- Consulta 2: Identificação de produtos com melhor custo-benefício por classe terapêutica
SELECT 
    ct.descricao_classe,
    p.nome_produto,
    p.apresentacao,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    a.aliquota,
    pf.pf_com_impostos AS preco_fabrica,
    pmvg.pmvg_com_impostos AS preco_pmvg,
    CASE 
        WHEN p.cap = 'Sim' AND pmvg.pmvg_com_impostos IS NOT NULL 
        THEN pmvg.pmvg_com_impostos
        ELSE pf.pf_com_impostos
    END AS preco_referencia_governo,
    p.cap,
    ROW_NUMBER() OVER (
        PARTITION BY ct.id_classe, a.aliquota 
        ORDER BY CASE 
            WHEN p.cap = 'Sim' AND pmvg.pmvg_com_impostos IS NOT NULL 
            THEN pmvg.pmvg_com_impostos
            ELSE pf.pf_com_impostos
        END ASC
    ) AS ranking_preco
FROM classes_terapeuticas ct
INNER JOIN produtos p ON ct.id_classe = p.id_classe
INNER JOIN substancias s ON p.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pf.id_aliquota = pmvg.id_aliquota
INNER JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
WHERE p.comercializacao_2024 = 'Sim'
    AND a.aliquota = 0
ORDER BY ct.descricao_classe, ranking_preco, preco_referencia_governo;

-- Consulta 3: Análise de impacto financeiro do CAP por laboratório
SELECT 
    l.nome_laboratorio,
    l.cnpj,
    COUNT(DISTINCT p.id_produto) AS total_produtos_cap,
    SUM(pf.pf_com_impostos) AS valor_total_pf,
    SUM(pmvg.pmvg_com_impostos) AS valor_total_pmvg,
    SUM(pf.pf_com_impostos - pmvg.pmvg_com_impostos) AS economia_total_cap,
    ROUND(AVG((pf.pf_com_impostos - pmvg.pmvg_com_impostos) * 100.0 / NULLIF(pf.pf_com_impostos, 0)), 2) AS desconto_medio_percentual,
    ROUND(SUM(pf.pf_com_impostos - pmvg.pmvg_com_impostos) * 100.0 / NULLIF(SUM(pf.pf_com_impostos), 0), 2) AS economia_percentual_total
FROM laboratorios l
INNER JOIN produtos p ON l.id_laboratorio = p.id_laboratorio
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
INNER JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pf.id_aliquota = pmvg.id_aliquota
INNER JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
WHERE p.cap = 'Sim'
    AND p.comercializacao_2024 = 'Sim'
    AND a.aliquota = 0
GROUP BY l.id_laboratorio, l.nome_laboratorio, l.cnpj
HAVING COUNT(DISTINCT p.id_produto) > 0
ORDER BY economia_total_cap DESC;

-- Consulta 4: Detecção de inconsistências e produtos que requerem atenção
SELECT 
    p.codigo_ggrem,
    p.nome_produto,
    p.apresentacao,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    rp.regime_preco,
    p.cap,
    CASE 
        WHEN p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL 
        THEN 'ALERTA: Produto com CAP mas sem PMVG cadastrado'
        WHEN p.cap = 'Não' AND pmvg.id_preco_pmvg IS NOT NULL 
        THEN 'INFO: PMVG cadastrado para produto sem CAP'
        WHEN pf.pf_com_impostos > 10000 
        THEN 'ALERTA: Preço muito alto (acima de R$ 10.000)'
        WHEN pf.pf_com_impostos IS NULL 
        THEN 'ERRO: Produto sem preço cadastrado'
        ELSE 'OK'
    END AS status_validacao,
    pf.pf_com_impostos AS preco_fabrica,
    pmvg.pmvg_com_impostos AS preco_pmvg,
    a.aliquota,
    p.data_atualizacao
FROM produtos p
INNER JOIN substancias s ON p.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN regimes_preco rp ON p.id_regime = rp.id_regime
LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pf.id_aliquota = pmvg.id_aliquota
LEFT JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
WHERE 
    (p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL)
    OR (pf.pf_com_impostos IS NULL)
    OR (pf.pf_com_impostos > 10000)
    OR (p.data_atualizacao < CURRENT_DATE - INTERVAL '1 year')
ORDER BY 
    CASE 
        WHEN CASE 
            WHEN p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL 
            THEN 'ALERTA: Produto com CAP mas sem PMVG cadastrado'
            WHEN pf.pf_com_impostos IS NULL 
            THEN 'ERRO: Produto sem preço cadastrado'
            WHEN pf.pf_com_impostos > 10000 
            THEN 'ALERTA: Preço muito alto (acima de R$ 10.000)'
            ELSE 'OK'
        END LIKE 'ERRO%' THEN 1
        WHEN CASE 
            WHEN p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL 
            THEN 'ALERTA: Produto com CAP mas sem PMVG cadastrado'
            ELSE 'OK'
        END LIKE 'ALERTA%' AND CASE 
            WHEN p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL 
            THEN 'ALERTA: Produto com CAP mas sem PMVG cadastrado'
            ELSE 'OK'
        END LIKE '%CAP%' THEN 2
        WHEN CASE 
            WHEN pf.pf_com_impostos > 10000 
            THEN 'ALERTA: Preço muito alto (acima de R$ 10.000)'
            ELSE 'OK'
        END LIKE 'ALERTA%' THEN 3
        ELSE 4
    END,
    p.nome_produto;

-- Consulta 5: Ranking de produtos mais caros por tipo, com análise comparativa
WITH precos_calculados AS (
    SELECT 
        p.id_produto,
        p.nome_produto,
        p.apresentacao,
        s.nome_substancia,
        l.nome_laboratorio,
        tp.tipo_produto,
        rp.regime_preco,
        a.aliquota,
        CASE 
            WHEN p.cap = 'Sim' AND pmvg.pmvg_com_impostos IS NOT NULL 
            THEN pmvg.pmvg_com_impostos
            ELSE pf.pf_com_impostos
        END AS preco_referencia,
        p.comercializacao_2024
    FROM produtos p
    INNER JOIN substancias s ON p.id_substancia = s.id_substancia
    INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
    INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
    INNER JOIN regimes_preco rp ON p.id_regime = rp.id_regime
    INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pf.id_aliquota = pmvg.id_aliquota
    INNER JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
    WHERE p.comercializacao_2024 = 'Sim'
        AND a.aliquota = 0
),
estatisticas_tipo AS (
    SELECT 
        tipo_produto,
        AVG(preco_referencia) AS preco_medio_tipo,
        STDDEV(preco_referencia) AS desvio_padrao_tipo,
        MIN(preco_referencia) AS preco_minimo_tipo,
        MAX(preco_referencia) AS preco_maximo_tipo
    FROM precos_calculados
    GROUP BY tipo_produto
)
SELECT 
    pc.nome_produto,
    pc.apresentacao,
    pc.nome_substancia,
    pc.nome_laboratorio,
    pc.tipo_produto,
    pc.regime_preco,
    pc.aliquota,
    pc.preco_referencia,
    et.preco_medio_tipo,
    et.preco_minimo_tipo,
    et.preco_maximo_tipo,
    ROUND((pc.preco_referencia - et.preco_medio_tipo) * 100.0 / NULLIF(et.preco_medio_tipo, 0), 2) AS percentual_acima_media,
    CASE 
        WHEN pc.preco_referencia >= et.preco_maximo_tipo * 0.9 
        THEN 'Muito Alto'
        WHEN pc.preco_referencia >= et.preco_medio_tipo * 1.5 
        THEN 'Alto'
        WHEN pc.preco_referencia <= et.preco_minimo_tipo * 1.1 
        THEN 'Baixo'
        ELSE 'Médio'
    END AS classificacao_preco,
    ROW_NUMBER() OVER (
        PARTITION BY pc.tipo_produto 
        ORDER BY pc.preco_referencia DESC
    ) AS ranking_tipo
FROM precos_calculados pc
INNER JOIN estatisticas_tipo et ON pc.tipo_produto = et.tipo_produto
WHERE pc.preco_referencia IS NOT NULL
ORDER BY pc.tipo_produto, pc.preco_referencia DESC
LIMIT 100;
