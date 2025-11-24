-- 5 Consultas SQL
-- PostgreSQL

-- Consulta 1: Substâncias com maior variação de preço entre laboratórios
-- Agrupa por substância (não por lab) para ver variação de mercado
SELECT 
    s.nome_substancia,
    COUNT(DISTINCT l.id_laboratorio) AS qtd_laboratorios,
    COUNT(DISTINCT p.id_produto) AS qtd_produtos,
    COUNT(DISTINCT tp.tipo_produto) AS qtd_tipos,
    ROUND(MIN(pf.pf_sem_impostos)::numeric, 2) AS preco_minimo,
    ROUND(MAX(pf.pf_sem_impostos)::numeric, 2) AS preco_maximo,
    ROUND(AVG(pf.pf_sem_impostos)::numeric, 2) AS preco_medio,
    ROUND((MAX(pf.pf_sem_impostos) - MIN(pf.pf_sem_impostos)) * 100.0 / NULLIF(MIN(pf.pf_sem_impostos), 0), 2) AS variacao_percentual
FROM substancias s
INNER JOIN produtos p ON s.id_substancia = p.id_substancia
INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
WHERE p.comercializacao_2024 = 'Sim' 
    AND pf.pf_sem_impostos IS NOT NULL
    AND tp.tipo_produto != '-'
GROUP BY s.nome_substancia
HAVING COUNT(DISTINCT l.id_laboratorio) > 1
ORDER BY variacao_percentual DESC;

-- Consulta 2: Produto mais barato por classe terapêutica (sem duplicatas)
-- Agrupa por produto para evitar duplicatas de apresentações diferentes
SELECT 
    ct.descricao_classe,
    p.nome_produto,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    ROUND(MIN(CASE WHEN p.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos ELSE pf.pf_sem_impostos END)::numeric, 2) AS preco_referencia_minimo,
    BOOL_OR(p.cap = 'Sim') AS possui_cap,
    COUNT(DISTINCT p.id_produto) AS qtd_apresentacoes
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
    AND tp.tipo_produto != '-'
GROUP BY ct.id_classe, ct.descricao_classe, p.nome_produto, s.nome_substancia, l.nome_laboratorio, tp.tipo_produto
ORDER BY ct.descricao_classe, preco_referencia_minimo ASC;

-- Consulta 3: Impacto financeiro do CAP por laboratório
-- ANVISA: Monitora eficácia do programa CAP e contribuição de cada laboratório
SELECT 
    l.nome_laboratorio,
    COUNT(DISTINCT p.id_produto) AS total_produtos_cap,
    ROUND(SUM(pf.pf_sem_impostos)::numeric, 2) AS valor_total_pf,
    ROUND(SUM(pmvg.pmvg_sem_impostos)::numeric, 2) AS valor_total_pmvg,
    ROUND(SUM(pf.pf_sem_impostos - pmvg.pmvg_sem_impostos)::numeric, 2) AS economia_total,
    ROUND(SUM(pf.pf_sem_impostos - pmvg.pmvg_sem_impostos) * 100.0 / NULLIF(SUM(pf.pf_sem_impostos), 0), 2) AS desconto_percentual
FROM laboratorios l
INNER JOIN produtos p ON l.id_laboratorio = p.id_laboratorio
INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
INNER JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
    AND (pf.id_aliquota = pmvg.id_aliquota OR (pf.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
WHERE p.cap = 'Sim' 
    AND p.comercializacao_2024 = 'Sim' 
    AND pf.pf_sem_impostos IS NOT NULL 
    AND pmvg.pmvg_sem_impostos IS NOT NULL
GROUP BY l.id_laboratorio, l.nome_laboratorio
ORDER BY economia_total DESC;

-- Consulta 4: Inconsistências agrupadas por tipo de problema
-- Identifica problemas de qualidade de dados para correção prioritária
SELECT 
    tipo_problema,
    COUNT(DISTINCT id_produto) AS qtd_produtos,
    COUNT(DISTINCT id_substancia) AS qtd_substancias,
    COUNT(DISTINCT id_laboratorio) AS qtd_laboratorios,
    ROUND(AVG(preco_fabrica)::numeric, 2) AS preco_medio,
    ROUND(MAX(preco_fabrica)::numeric, 2) AS preco_maximo
FROM (
    SELECT 
        p.id_produto,
        s.id_substancia,
        l.id_laboratorio,
        pf.pf_sem_impostos AS preco_fabrica,
        CASE 
            WHEN pf.pf_sem_impostos IS NULL THEN 'ERRO: Sem preço cadastrado'
            WHEN p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL THEN 'ALERTA: CAP sem PMVG'
            WHEN pf.pf_sem_impostos > 10000 THEN 'ALERTA: Preço acima de R$ 10.000'
            WHEN p.data_atualizacao < CURRENT_DATE - INTERVAL '1 year' THEN 'INFO: Dados desatualizados'
            ELSE 'OK'
        END AS tipo_problema
    FROM produtos p
    INNER JOIN substancias s ON p.id_substancia = s.id_substancia
    INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
    LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
        AND (pf.id_aliquota = pmvg.id_aliquota OR (pf.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    WHERE pf.pf_sem_impostos IS NULL
        OR (p.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL)
        OR pf.pf_sem_impostos > 10000
        OR p.data_atualizacao < CURRENT_DATE - INTERVAL '1 year'
) subq
GROUP BY tipo_problema
ORDER BY 
    CASE 
        WHEN tipo_problema LIKE 'ERRO%' THEN 1
        WHEN tipo_problema LIKE 'ALERTA%CAP%' THEN 2
        WHEN tipo_problema LIKE 'ALERTA%Preço%' THEN 3
        ELSE 4
    END,
    qtd_produtos DESC;

-- Consulta 5: Produtos mais caros por tipo (uma apresentação por produto)
-- Agrupa por produto para evitar múltiplas apresentações do mesmo medicamento
WITH precos_por_produto AS (
    SELECT 
        p.nome_produto,
        s.nome_substancia,
        l.nome_laboratorio,
        tp.tipo_produto,
        MAX(CASE WHEN p.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos ELSE pf.pf_sem_impostos END) AS preco_referencia
    FROM produtos p
    INNER JOIN substancias s ON p.id_substancia = s.id_substancia
    INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
    INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
    INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
        AND (pf.id_aliquota = pmvg.id_aliquota OR (pf.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    WHERE p.comercializacao_2024 = 'Sim' 
        AND pf.pf_sem_impostos IS NOT NULL
        AND tp.tipo_produto != '-'
    GROUP BY p.nome_produto, s.nome_substancia, l.nome_laboratorio, tp.tipo_produto
),
estatisticas_tipo AS (
    SELECT 
        tipo_produto,
        AVG(preco_referencia) AS preco_medio,
        MIN(preco_referencia) AS preco_minimo,
        MAX(preco_referencia) AS preco_maximo,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY preco_referencia) AS preco_mediana
    FROM precos_por_produto
    GROUP BY tipo_produto
)
SELECT 
    pc.nome_produto,
    pc.nome_substancia,
    pc.nome_laboratorio,
    pc.tipo_produto,
    ROUND(pc.preco_referencia::numeric, 2) AS preco,
    ROUND(et.preco_medio::numeric, 2) AS media_tipo,
    ROUND(et.preco_mediana::numeric, 2) AS mediana_tipo,
    CASE 
        WHEN et.preco_medio > 0 AND (pc.preco_referencia - et.preco_medio) / et.preco_medio <= 5.0 THEN
            ROUND((pc.preco_referencia - et.preco_medio) * 100.0 / et.preco_medio, 2)
        WHEN et.preco_medio > 0 THEN
            ROUND((pc.preco_referencia - et.preco_medio) * 100.0 / et.preco_medio, 0)
        ELSE NULL
    END AS pct_acima_media,
    CASE 
        WHEN pc.preco_referencia >= et.preco_maximo * 0.9 THEN 'Muito Alto'
        WHEN pc.preco_referencia >= et.preco_mediana * 2.0 THEN 'Alto (outlier)'
        WHEN pc.preco_referencia >= et.preco_medio * 1.5 THEN 'Alto'
        WHEN pc.preco_referencia <= et.preco_minimo * 1.1 THEN 'Baixo'
        ELSE 'Médio'
    END AS classificacao
FROM precos_por_produto pc
INNER JOIN estatisticas_tipo et ON pc.tipo_produto = et.tipo_produto
WHERE pc.preco_referencia IS NOT NULL
ORDER BY pc.tipo_produto, pc.preco_referencia DESC
LIMIT 100;
