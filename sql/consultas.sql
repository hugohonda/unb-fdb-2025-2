-- 5 Consultas SQL Complexas - Perspectiva ANVISA/Farmácia
-- PostgreSQL

-- Consulta 1: Substâncias com maior variação de preço entre laboratórios
-- Identifica possíveis abusos de preço e necessidade de regulação
-- Agrupa por substância (não por lab) para ver variação de mercado
WITH precos_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto)
        p.id_produto,
        p.id_substancia,
        p.id_laboratorio,
        p.id_tipo,
        pf.pf_sem_impostos
    FROM produtos p
    INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
    INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    WHERE p.comercializacao_2024 = 'Sim' 
        AND pf.pf_sem_impostos IS NOT NULL
        AND tp.tipo_produto != '-'
    ORDER BY p.id_produto, pf.data_vigencia DESC
)
SELECT 
    s.nome_substancia,
    COUNT(DISTINCT pmr.id_laboratorio) AS qtd_laboratorios,
    COUNT(DISTINCT pmr.id_produto) AS qtd_produtos,
    COUNT(DISTINCT tp.tipo_produto) AS qtd_tipos,
    ROUND(MIN(pmr.pf_sem_impostos)::numeric, 2) AS preco_minimo,
    ROUND(MAX(pmr.pf_sem_impostos)::numeric, 2) AS preco_maximo,
    ROUND(AVG(pmr.pf_sem_impostos)::numeric, 2) AS preco_medio,
    ROUND((MAX(pmr.pf_sem_impostos) - MIN(pmr.pf_sem_impostos)) * 100.0 / NULLIF(MIN(pmr.pf_sem_impostos), 0), 2) AS variacao_percentual
FROM substancias s
INNER JOIN precos_mais_recentes pmr ON s.id_substancia = pmr.id_substancia
INNER JOIN laboratorios l ON pmr.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON pmr.id_tipo = tp.id_tipo
GROUP BY s.nome_substancia
HAVING COUNT(DISTINCT pmr.id_laboratorio) > 1
ORDER BY variacao_percentual DESC;

-- Consulta 2: Produto mais barato por classe terapêutica (sem duplicatas)
-- Identifica melhor opção de custo-benefício para compras governamentais
-- Agrupa por produto para evitar duplicatas de apresentações diferentes
WITH precos_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto)
        p.id_produto,
        p.id_classe,
        p.nome_produto,
        p.id_substancia,
        p.id_laboratorio,
        p.id_tipo,
        p.cap,
        pf.pf_sem_impostos,
        pf.id_aliquota
    FROM produtos p
    INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
    INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    WHERE p.comercializacao_2024 = 'Sim' 
        AND pf.pf_sem_impostos IS NOT NULL
        AND tp.tipo_produto != '-'
    ORDER BY p.id_produto, pf.data_vigencia DESC
),
pmvg_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto, pmr.id_aliquota)
        p.id_produto,
        pmr.id_aliquota,
        pmvg.pmvg_sem_impostos
    FROM produtos p
    INNER JOIN precos_mais_recentes pmr ON p.id_produto = pmr.id_produto
    INNER JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
        AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    WHERE pmvg.pmvg_sem_impostos IS NOT NULL
    ORDER BY p.id_produto, pmr.id_aliquota, pmvg.data_vigencia DESC
)
SELECT 
    ct.descricao_classe,
    pmr.nome_produto,
    s.nome_substancia,
    l.nome_laboratorio,
    tp.tipo_produto,
    ROUND(MIN(CASE WHEN pmr.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos ELSE pmr.pf_sem_impostos END)::numeric, 2) AS preco_referencia_minimo,
    BOOL_OR(pmr.cap = 'Sim') AS possui_cap,
    COUNT(DISTINCT pmr.id_produto) AS qtd_apresentacoes
FROM classes_terapeuticas ct
INNER JOIN precos_mais_recentes pmr ON ct.id_classe = pmr.id_classe
INNER JOIN substancias s ON pmr.id_substancia = s.id_substancia
INNER JOIN laboratorios l ON pmr.id_laboratorio = l.id_laboratorio
INNER JOIN tipos_produto tp ON pmr.id_tipo = tp.id_tipo
LEFT JOIN pmvg_mais_recentes pmvg ON pmr.id_produto = pmvg.id_produto 
    AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
GROUP BY ct.id_classe, ct.descricao_classe, pmr.nome_produto, s.nome_substancia, l.nome_laboratorio, tp.tipo_produto
ORDER BY ct.descricao_classe, preco_referencia_minimo ASC;

-- Consulta 3: Impacto financeiro do CAP por laboratório
-- Monitora eficácia do programa CAP e contribuição de cada laboratório
-- Mostra desconto médio por produto (não ponderado) e faixa de desconto para identificar variações
WITH precos_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto)
        p.id_produto,
        p.id_laboratorio,
        p.cap,
        pf.pf_sem_impostos,
        pf.id_aliquota
    FROM produtos p
    INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    WHERE p.cap = 'Sim' 
        AND p.comercializacao_2024 = 'Sim' 
        AND pf.pf_sem_impostos IS NOT NULL
    ORDER BY p.id_produto, pf.data_vigencia DESC
),
pmvg_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto, pmr.id_aliquota)
        p.id_produto,
        pmr.id_aliquota,
        pmvg.pmvg_sem_impostos
    FROM produtos p
    INNER JOIN precos_mais_recentes pmr ON p.id_produto = pmr.id_produto
    INNER JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
        AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    WHERE pmvg.pmvg_sem_impostos IS NOT NULL
    ORDER BY p.id_produto, pmr.id_aliquota, pmvg.data_vigencia DESC
)
SELECT 
    l.nome_laboratorio,
    COUNT(DISTINCT pmr.id_produto) AS total_produtos_cap,
    ROUND(SUM(pmr.pf_sem_impostos)::numeric, 2) AS valor_total_pf,
    ROUND(SUM(pmvg.pmvg_sem_impostos)::numeric, 2) AS valor_total_pmvg,
    ROUND(SUM(pmr.pf_sem_impostos - pmvg.pmvg_sem_impostos)::numeric, 2) AS economia_total,
    ROUND(AVG((pmr.pf_sem_impostos - pmvg.pmvg_sem_impostos) * 100.0 / NULLIF(pmr.pf_sem_impostos, 0))::numeric, 2) AS desconto_medio_produto,
    ROUND(MIN((pmr.pf_sem_impostos - pmvg.pmvg_sem_impostos) * 100.0 / NULLIF(pmr.pf_sem_impostos, 0))::numeric, 2) AS desconto_minimo,
    ROUND(MAX((pmr.pf_sem_impostos - pmvg.pmvg_sem_impostos) * 100.0 / NULLIF(pmr.pf_sem_impostos, 0))::numeric, 2) AS desconto_maximo
FROM laboratorios l
INNER JOIN precos_mais_recentes pmr ON l.id_laboratorio = pmr.id_laboratorio
INNER JOIN pmvg_mais_recentes pmvg ON pmr.id_produto = pmvg.id_produto 
    AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
GROUP BY l.id_laboratorio, l.nome_laboratorio
ORDER BY economia_total DESC;

-- Consulta 4: Inconsistências agrupadas por tipo de problema
-- Identifica problemas de qualidade de dados para correção prioritária
WITH precos_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto)
        p.id_produto,
        p.id_substancia,
        p.id_laboratorio,
        p.cap,
        p.data_atualizacao,
        pf.pf_sem_impostos,
        pf.id_aliquota
    FROM produtos p
    LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    ORDER BY p.id_produto, pf.data_vigencia DESC NULLS LAST
),
pmvg_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto, pmr.id_aliquota)
        p.id_produto,
        pmr.id_aliquota,
        pmvg.id_preco_pmvg
    FROM produtos p
    INNER JOIN precos_mais_recentes pmr ON p.id_produto = pmr.id_produto
    LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
        AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    ORDER BY p.id_produto, pmr.id_aliquota, pmvg.data_vigencia DESC NULLS LAST
)
SELECT 
    tipo_problema,
    COUNT(DISTINCT id_produto) AS qtd_produtos,
    COUNT(DISTINCT id_substancia) AS qtd_substancias,
    COUNT(DISTINCT id_laboratorio) AS qtd_laboratorios,
    ROUND(AVG(preco_fabrica)::numeric, 2) AS preco_medio,
    ROUND(MAX(preco_fabrica)::numeric, 2) AS preco_maximo
FROM (
    SELECT 
        pmr.id_produto,
        pmr.id_substancia,
        pmr.id_laboratorio,
        pmr.pf_sem_impostos AS preco_fabrica,
        CASE 
            WHEN pmr.pf_sem_impostos IS NULL THEN 'ERRO: Sem preço cadastrado'
            WHEN pmr.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL THEN 'ALERTA: CAP sem PMVG'
            WHEN pmr.pf_sem_impostos > 10000 THEN 'ALERTA: Preço acima de R$ 10.000'
            WHEN pmr.data_atualizacao < CURRENT_DATE - INTERVAL '1 year' THEN 'INFO: Dados desatualizados'
            ELSE 'OK'
        END AS tipo_problema
    FROM precos_mais_recentes pmr
    LEFT JOIN pmvg_mais_recentes pmvg ON pmr.id_produto = pmvg.id_produto 
        AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    WHERE pmr.pf_sem_impostos IS NULL
        OR (pmr.cap = 'Sim' AND pmvg.id_preco_pmvg IS NULL)
        OR pmr.pf_sem_impostos > 10000
        OR pmr.data_atualizacao < CURRENT_DATE - INTERVAL '1 year'
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
-- Identifica outliers de preço para análise de regulação
-- Agrupa por produto para evitar múltiplas apresentações do mesmo medicamento
WITH precos_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto)
        p.id_produto,
        p.nome_produto,
        p.id_substancia,
        p.id_laboratorio,
        p.id_tipo,
        p.cap,
        pf.pf_sem_impostos,
        pf.id_aliquota
    FROM produtos p
    INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
    INNER JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    WHERE p.comercializacao_2024 = 'Sim' 
        AND pf.pf_sem_impostos IS NOT NULL
        AND tp.tipo_produto != '-'
    ORDER BY p.id_produto, pf.data_vigencia DESC
),
pmvg_mais_recentes AS (
    SELECT DISTINCT ON (p.id_produto, pmr.id_aliquota)
        p.id_produto,
        pmr.id_aliquota,
        pmvg.pmvg_sem_impostos
    FROM produtos p
    INNER JOIN precos_mais_recentes pmr ON p.id_produto = pmr.id_produto
    LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto 
        AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    ORDER BY p.id_produto, pmr.id_aliquota, pmvg.data_vigencia DESC NULLS LAST
),
precos_por_produto AS (
    SELECT 
        pmr.nome_produto,
        s.nome_substancia,
        l.nome_laboratorio,
        tp.tipo_produto,
        MAX(CASE WHEN pmr.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos ELSE pmr.pf_sem_impostos END) AS preco_referencia
    FROM precos_mais_recentes pmr
    INNER JOIN substancias s ON pmr.id_substancia = s.id_substancia
    INNER JOIN laboratorios l ON pmr.id_laboratorio = l.id_laboratorio
    INNER JOIN tipos_produto tp ON pmr.id_tipo = tp.id_tipo
    LEFT JOIN pmvg_mais_recentes pmvg ON pmr.id_produto = pmvg.id_produto 
        AND (pmr.id_aliquota = pmvg.id_aliquota OR (pmr.id_aliquota IS NULL AND pmvg.id_aliquota IS NULL))
    GROUP BY pmr.nome_produto, s.nome_substancia, l.nome_laboratorio, tp.tipo_produto
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
