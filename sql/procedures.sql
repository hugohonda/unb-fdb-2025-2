-- Procedures com comandos condicionais
-- PostgreSQL

-- Procedure: Atualizar preço de um produto com validações
CREATE OR REPLACE PROCEDURE sp_atualizar_preco_produto(
    p_codigo_ggrem VARCHAR(20),
    p_id_aliquota INTEGER,
    p_tipo_preco tipo_preco,
    p_novo_valor DECIMAL(10,2),
    p_usuario VARCHAR(100),
    INOUT p_resultado VARCHAR(255) DEFAULT ''
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_id_produto INTEGER;
    v_valor_anterior DECIMAL(10,2);
    v_cap tipo_sim_nao;
    v_percentual_variacao DECIMAL(10,2);
BEGIN
    -- Verifica se o produto existe
    SELECT id_produto, cap INTO v_id_produto, v_cap
    FROM produtos
    WHERE codigo_ggrem = p_codigo_ggrem;
    
    IF v_id_produto IS NULL THEN
        p_resultado := 'ERRO: Produto não encontrado';
        RETURN;
    END IF;
    
    -- Validações condicionais baseadas no tipo de preço
    IF p_tipo_preco = 'PMVG' THEN
        -- Se é PMVG, verifica se o produto tem CAP
        IF v_cap = 'Não' THEN
            SELECT pf_com_impostos INTO v_valor_anterior
            FROM precos_fabrica
            WHERE id_produto = v_id_produto AND id_aliquota = p_id_aliquota
            LIMIT 1;
            
            -- Valida se o PMVG não excede o PF (exceto quando há desconto CAP)
            IF v_valor_anterior IS NOT NULL AND p_novo_valor > v_valor_anterior THEN
                p_resultado := 'ERRO: PMVG (' || p_novo_valor || ') não pode ser maior que PF (' || v_valor_anterior || ') para produtos sem CAP';
                RETURN;
            END IF;
        END IF;
        
        -- Atualiza ou insere PMVG
        INSERT INTO precos_pmvg (id_produto, id_aliquota, pmvg_com_impostos, data_vigencia)
        VALUES (v_id_produto, p_id_aliquota, p_novo_valor, CURRENT_DATE)
        ON CONFLICT (id_produto, id_aliquota, data_vigencia)
        DO UPDATE SET 
            pmvg_com_impostos = EXCLUDED.pmvg_com_impostos,
            data_vigencia = EXCLUDED.data_vigencia;
        
        -- Registra no histórico
        SELECT pmvg_com_impostos INTO v_valor_anterior
        FROM precos_pmvg
        WHERE id_produto = v_id_produto AND id_aliquota = p_id_aliquota
        ORDER BY data_vigencia DESC
        LIMIT 1;
        
    ELSIF p_tipo_preco = 'PF' THEN
        -- Valida variação percentual do preço
        SELECT pf_com_impostos INTO v_valor_anterior
        FROM precos_fabrica
        WHERE id_produto = v_id_produto AND id_aliquota = p_id_aliquota
        ORDER BY data_vigencia DESC
        LIMIT 1;
        
        IF v_valor_anterior IS NOT NULL THEN
            v_percentual_variacao := ABS((p_novo_valor - v_valor_anterior) / v_valor_anterior * 100);
            
            -- Se variação maior que 50%, avisa
            IF v_percentual_variacao > 50 THEN
                p_resultado := 'AVISO: Variação de ' || ROUND(v_percentual_variacao, 2) || '% detectada. Prosseguindo com atualização.';
            END IF;
        END IF;
        
        -- Atualiza ou insere PF
        INSERT INTO precos_fabrica (id_produto, id_aliquota, pf_com_impostos, data_vigencia)
        VALUES (v_id_produto, p_id_aliquota, p_novo_valor, CURRENT_DATE)
        ON CONFLICT (id_produto, id_aliquota, data_vigencia)
        DO UPDATE SET 
            pf_com_impostos = EXCLUDED.pf_com_impostos,
            data_vigencia = EXCLUDED.data_vigencia;
        
        -- Registra no histórico
        SELECT pf_com_impostos INTO v_valor_anterior
        FROM precos_fabrica
        WHERE id_produto = v_id_produto AND id_aliquota = p_id_aliquota
        ORDER BY data_vigencia DESC
        LIMIT 1;
    ELSE
        p_resultado := 'ERRO: Tipo de preço inválido';
        RETURN;
    END IF;
    
    -- Registra alteração no histórico se houve mudança
    IF v_valor_anterior IS NULL OR v_valor_anterior != p_novo_valor THEN
        INSERT INTO historico_precos (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
        VALUES (v_id_produto, p_tipo_preco, p_id_aliquota, v_valor_anterior, p_novo_valor, p_usuario);
    END IF;
    
    IF p_resultado NOT LIKE 'ERRO%' AND p_resultado NOT LIKE 'AVISO%' THEN
        p_resultado := 'SUCESSO: Preço atualizado. Valor anterior: ' || 
                      COALESCE(v_valor_anterior::TEXT, 'N/A') || 
                      ', Novo valor: ' || p_novo_valor;
    END IF;
END;
$$;

-- Procedure: Buscar produtos por critérios com filtros condicionais
CREATE OR REPLACE FUNCTION sp_buscar_produtos(
    p_substancia VARCHAR(255) DEFAULT NULL,
    p_laboratorio VARCHAR(255) DEFAULT NULL,
    p_tipo_produto VARCHAR(50) DEFAULT NULL,
    p_com_cap BOOLEAN DEFAULT NULL,
    p_aliquota DECIMAL(5,2) DEFAULT NULL,
    p_preco_maximo DECIMAL(10,2) DEFAULT NULL,
    p_ordenar_por VARCHAR(50) DEFAULT 'produto'
)
RETURNS TABLE (
    codigo_ggrem VARCHAR(20),
    nome_produto VARCHAR(255),
    apresentacao TEXT,
    nome_substancia VARCHAR(255),
    nome_laboratorio VARCHAR(255),
    tipo_produto VARCHAR(50),
    regime_preco VARCHAR(50),
    cap tipo_sim_nao,
    comercializacao_2024 tipo_sim_nao,
    aliquota DECIMAL(5,2),
    preco_fabrica DECIMAL(10,2),
    preco_pmvg DECIMAL(10,2),
    preco_referencia DECIMAL(10,2)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        p.codigo_ggrem,
        p.nome_produto,
        p.apresentacao,
        s.nome_substancia,
        l.nome_laboratorio,
        tp.tipo_produto,
        rp.regime_preco,
        p.cap,
        p.comercializacao_2024,
        a.aliquota,
        pf.pf_com_impostos AS preco_fabrica,
        pmvg.pmvg_com_impostos AS preco_pmvg,
        CASE 
            WHEN p.cap = 'Sim' AND pmvg.pmvg_com_impostos IS NOT NULL THEN pmvg.pmvg_com_impostos
            ELSE pf.pf_com_impostos
        END AS preco_referencia
    FROM produtos p
    INNER JOIN substancias s ON p.id_substancia = s.id_substancia
    INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
    INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
    INNER JOIN regimes_preco rp ON p.id_regime = rp.id_regime
    LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto
    LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pf.id_aliquota = pmvg.id_aliquota
    LEFT JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
    WHERE 
        (p_substancia IS NULL OR s.nome_substancia ILIKE '%' || p_substancia || '%')
        AND (p_laboratorio IS NULL OR l.nome_laboratorio ILIKE '%' || p_laboratorio || '%')
        AND (p_tipo_produto IS NULL OR tp.tipo_produto = p_tipo_produto)
        AND (p_com_cap IS NULL OR (p_com_cap = TRUE AND p.cap = 'Sim') OR (p_com_cap = FALSE AND p.cap = 'Não'))
        AND (p_aliquota IS NULL OR a.aliquota = p_aliquota)
        AND (p_preco_maximo IS NULL OR 
             (CASE 
                WHEN p.cap = 'Sim' AND pmvg.pmvg_com_impostos IS NOT NULL THEN pmvg.pmvg_com_impostos
                ELSE pf.pf_com_impostos
             END) <= p_preco_maximo)
    ORDER BY 
        CASE 
            WHEN p_ordenar_por = 'preco' THEN 
                CASE 
                    WHEN p.cap = 'Sim' AND pmvg.pmvg_com_impostos IS NOT NULL THEN pmvg.pmvg_com_impostos
                    ELSE pf.pf_com_impostos
                END
            ELSE 0
        END,
        CASE WHEN p_ordenar_por = 'laboratorio' THEN l.nome_laboratorio ELSE '' END,
        CASE WHEN p_ordenar_por != 'preco' AND p_ordenar_por != 'laboratorio' THEN p.nome_produto ELSE '' END;
END;
$$;

