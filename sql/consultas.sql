-- 5 Consultas SQL Complexas
-- PostgreSQL

-- Consulta 1: Análise comparativa de preços entre laboratórios para a mesma substância
SELECT 
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    COUNT(DISTINCT p.id_produto) AS qtd_apresentacoes,
    ROUND(AVG(pf.pf_sem_impostos)::numeric, 2) AS preco_medio_pf,
    ROUND(MIN(pf.pf_sem_impostos)::numeric, 2) AS preco_minimo_pf,
    ROUND(MAX(pf.pf_sem_impostos)::numeric, 2) AS preco_maximo_pf,
    ROUND(STDDEV(pf.pf_sem_impostos)::numeric, 2) AS desvio_padrao_preco,
    ROUND((MAX(pf.pf_sem_impostos) - MIN(pf.pf_sem_impostos)) * 100.0 / NULLIF(MIN(pf.pf_sem_impostos), 0), 2) AS variacao_percentual
FROM substancias s
INNER JOIN produtos p ON s.id_substancia = p.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
WHERE p.comercializacao_2024 = 'Sim'
    AND pf.pf_sem_impostos IS NOT NULL
GROUP BY s.nome_substancia, l.nome_laboratorio, tp.tipo_produto
ORDER BY s.nome_substancia, variacao_percentual DESC;

-- Consulta 2: Identificação de produtos com melhor custo-benefício por classe terapêutica
SELECT 
    ct.descricao_classe,
    p.nome_produto,
    p.apresentacao,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    ROUND(pf.pf_sem_impostos::numeric, 2) AS preco_fabrica,
    ROUND(pmvg.pmvg_sem_impostos::numeric, 2) AS preco_pmvg,
    ROUND((CASE WHEN p.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos ELSE pf.pf_sem_impostos END)::numeric, 2) AS preco_referencia_governo,
    p.cap,
    ROW_NUMBER() OVER (
        PARTITION BY ct.id_classe 
        ORDER BY CASE WHEN p.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos ELSE pf.pf_sem_impostos END ASC
    ) AS ranking_preco
FROM classes_terapeuticas ct
INNER JOIN produtos p ON ct.id_classe = p.id_classe
INNER JOIN substancias s ON p.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
    AND (pf.id_aliquota = pmvg.id_aliquota OR (pf.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
WHERE p.comercializacao_2024 = 'Sim'
    AND pf.pf_sem_impostos IS NOT NULL
ORDER BY ct.descricao_classe, ranking_preco;

-- Consulta 3: Análise de impacto financeiro do CAP por laboratório
SELECT 
    l.nome_laboratorio,
    l.cnpj,
    COUNT(DISTINCT p.id_produto) AS total_produtos_cap,
    ROUND(SUM(pf.pf_sem_impostos)::numeric, 2) AS valor_total_pf,
    ROUND(SUM(pmvg.pmvg_sem_impostos)::numeric, 2) AS valor_total_pmvg,
    ROUND(SUM(pf.pf_sem_impostos - pmvg.pmvg_sem_impostos)::numeric, 2) AS economia_total_cap,
    ROUND(AVG((pf.pf_sem_impostos - pmvg.pmvg_sem_impostos) * 100.0 / NULLIF(pf.pf_sem_impostos, 0)), 2) AS desconto_medio_percentual,
    ROUND(SUM(pf.pf_sem_impostos - pmvg.pmvg_sem_impostos) * 100.0 / NULLIF(SUM(pf.pf_sem_impostos), 0), 2) AS economia_percentual_total
FROM laboratorios l
INNER JOIN produtos p ON l.id_laboratorio = p.id_laboratorio
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
INNER JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
    AND (pf.id_aliquota = pmvg.id_aliquota OR (pf.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
WHERE p.cap = 'Sim'
    AND p.comercializacao_2024 = 'Sim'
    AND pf.pf_sem_impostos IS NOT NULL
    AND pmvg.pmvg_sem_impostos IS NOT NULL
GROUP BY l.id_laboratorio, l.nome_laboratorio, l.cnpj
ORDER BY economia_total_cap DESC;

-- Consulta 4: Detecção de inconsistências e produtos que requerem atenção
SELECT 
    p.codigo_ggrem,
    p.nome_produto,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    p.cap,
    CASE 
        WHEN pf.pf_sem_impostos IS NULL THEN 'ERRO: Produto sem preço cadastrado'
        WHEN p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL THEN 'ALERTA: Produto com CAP mas sem PMVG'
        WHEN pf.pf_sem_impostos > 10000 THEN 'ALERTA: Preço muito alto (acima de R$ 10.000)'
        WHEN p.data_atualizacao < CURRENT_DATE - INTERVAL '1 year' THEN 'INFO: Dados desatualizados'
        ELSE 'OK'
    END AS status_validacao,
    ROUND(pf.pf_sem_impostos::numeric, 2) AS preco_fabrica,
    ROUND(pmvg.pmvg_sem_impostos::numeric, 2) AS preco_pmvg,
    p.data_atualizacao
FROM produtos p
INNER JOIN substancias s ON p.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
    AND (pf.id_aliquota = pmvg.id_aliquota OR (pf.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
WHERE pf.pf_sem_impostos IS NULL
    OR (p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL)
    OR pf.pf_sem_impostos > 10000
    OR p.data_atualizacao < CURRENT_DATE - INTERVAL '1 year'
ORDER BY 
    CASE 
        WHEN pf.pf_sem_impostos IS NULL THEN 1
        WHEN p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL THEN 2
        WHEN pf.pf_sem_impostos > 10000 THEN 3
        ELSE 4
    END,
    p.nome_produto;

-- Consulta 5: Ranking de produtos mais caros por tipo, com análise comparativa
WITH precos_calculados AS (
    SELECT 
        p.id_produto,
        p.nome_produto,
        s.nome_substancia,
        l.nome_laboratorio,
        tp.tipo_produto,
        CASE WHEN p.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos ELSE pf.pf_sem_impostos END AS preco_referencia
    FROM produtos p
    INNER JOIN substancias s ON p.id_substancia = s.id_substancia
    INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
    INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
    INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
        AND (pf.id_aliquota = pmvg.id_aliquota OR (pf.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    WHERE p.comercializacao_2024 = 'Sim'
        AND pf.pf_sem_impostos IS NOT NULL
),
estatisticas_tipo AS (
    SELECT 
        tipo_produto,
        AVG(preco_referencia) AS preco_medio_tipo,
        MIN(preco_referencia) AS preco_minimo_tipo,
        MAX(preco_referencia) AS preco_maximo_tipo
    FROM precos_calculados
    GROUP BY tipo_produto
)
SELECT 
    pc.nome_produto,
    pc.nome_substancia,
    pc.nome_laboratorio,
    pc.tipo_produto,
    ROUND(pc.preco_referencia::numeric, 2) AS preco_referencia,
    ROUND(et.preco_medio_tipo::numeric, 2) AS preco_medio_tipo,
    ROUND(et.preco_minimo_tipo::numeric, 2) AS preco_minimo_tipo,
    ROUND(et.preco_maximo_tipo::numeric, 2) AS preco_maximo_tipo,
    CASE 
        WHEN et.preco_medio_tipo > 0 AND (pc.preco_referencia - et.preco_medio_tipo) / et.preco_medio_tipo <= 5.0 THEN
            ROUND((pc.preco_referencia - et.preco_medio_tipo) * 100.0 / et.preco_medio_tipo, 2)
        WHEN et.preco_medio_tipo > 0 THEN
            ROUND((pc.preco_referencia - et.preco_medio_tipo) * 100.0 / et.preco_medio_tipo, 0)
        ELSE NULL
    END AS percentual_acima_media,
    CASE 
        WHEN pc.preco_referencia >= et.preco_maximo_tipo * 0.9 THEN 'Muito Alto'
        WHEN pc.preco_referencia >= et.preco_medio_tipo * 1.5 THEN 'Alto'
        WHEN pc.preco_referencia <= et.preco_minimo_tipo * 1.1 THEN 'Baixo'
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
