from datetime import date


def atualizar_preco_produto(connection, codigo_ggrem, id_aliquota, tipo_preco, novo_valor, usuario):
    """
    Atualiza preço de um produto com validações condicionais
    
    Args:
        connection: Conexão SQLite
        codigo_ggrem: Código GGREM do produto
        id_aliquota: ID da alíquota ICMS
        tipo_preco: 'PF' ou 'PMVG'
        novo_valor: Novo valor do preço
        usuario: Nome do usuário que fez a alteração
    
    Returns:
        str: Mensagem de resultado da operação
    """
    cursor = connection.cursor()
    
    try:
        # Verifica se o produto existe
        cursor.execute("SELECT COUNT(*) as count FROM produtos WHERE codigo_ggrem = ?", (codigo_ggrem,))
        resultado = cursor.fetchone()
        if resultado['count'] == 0:
            return 'ERRO: Produto não encontrado'
        
        # Obtém informações do produto
        cursor.execute("""
            SELECT p.id_produto, p.cap, rp.regime_preco 
            FROM produtos p
            INNER JOIN regimes_preco rp ON p.id_regime = rp.id_regime
            WHERE p.codigo_ggrem = ?
        """, (codigo_ggrem,))
        
        produto = cursor.fetchone()
        if not produto:
            return 'ERRO: Produto não encontrado'
        
        id_produto = produto['id_produto']
        cap = produto['cap']
        
        valor_anterior = None
        
        if tipo_preco == 'PMVG':
            # Se é PMVG, verifica se o produto tem CAP
            if cap == 'Não':
                # Busca valor PF para comparação
                cursor.execute("""
                    SELECT pf_com_impostos 
                    FROM precos_fabrica
                    WHERE id_produto = ? AND id_aliquota = ?
                    LIMIT 1
                """, (id_produto, id_aliquota))
                pf_result = cursor.fetchone()
                
                if pf_result and pf_result['pf_com_impostos']:
                    valor_anterior_pf = pf_result['pf_com_impostos']
                    # Valida se o PMVG não excede o PF
                    if novo_valor > valor_anterior_pf:
                        return f'ERRO: PMVG ({novo_valor}) não pode ser maior que PF ({valor_anterior_pf}) para produtos sem CAP'
            
            # Busca valor anterior de PMVG
            cursor.execute("""
                SELECT pmvg_com_impostos 
                FROM precos_pmvg
                WHERE id_produto = ? AND id_aliquota = ?
                LIMIT 1
            """, (id_produto, id_aliquota))
            pmvg_result = cursor.fetchone()
            if pmvg_result:
                valor_anterior = pmvg_result['pmvg_com_impostos']
            
            # Atualiza ou insere PMVG
            cursor.execute("""
                INSERT OR REPLACE INTO precos_pmvg 
                    (id_produto, id_aliquota, pmvg_com_impostos, data_vigencia)
                VALUES (?, ?, ?, ?)
            """, (id_produto, id_aliquota, novo_valor, date.today().isoformat()))
            
        elif tipo_preco == 'PF':
            # Busca valor anterior
            cursor.execute("""
                SELECT pf_com_impostos 
                FROM precos_fabrica
                WHERE id_produto = ? AND id_aliquota = ?
                LIMIT 1
            """, (id_produto, id_aliquota))
            pf_result = cursor.fetchone()
            
            if pf_result and pf_result['pf_com_impostos']:
                valor_anterior = pf_result['pf_com_impostos']
                # Valida variação percentual
                variacao = abs((novo_valor - valor_anterior) / valor_anterior * 100)
                if variacao > 50:
                    print(f'AVISO: Variação de {variacao:.2f}% detectada. Prosseguindo com atualização.')
            
            # Atualiza ou insere PF
            cursor.execute("""
                INSERT OR REPLACE INTO precos_fabrica 
                    (id_produto, id_aliquota, pf_com_impostos, data_vigencia)
                VALUES (?, ?, ?, ?)
            """, (id_produto, id_aliquota, novo_valor, date.today().isoformat()))
        else:
            return 'ERRO: Tipo de preço inválido'
        
        # Registra no histórico se houve mudança
        if valor_anterior is None or valor_anterior != novo_valor:
            cursor.execute("""
                INSERT INTO historico_precos 
                    (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (id_produto, tipo_preco, id_aliquota, valor_anterior, novo_valor, usuario))
        
        connection.commit()
        
        valor_anterior_str = str(valor_anterior) if valor_anterior is not None else 'N/A'
        return f'SUCESSO: Preço atualizado. Valor anterior: {valor_anterior_str}, Novo valor: {novo_valor}'
        
    except Exception as e:
        connection.rollback()
        return f'ERRO: {str(e)}'


def buscar_produtos(connection, substancia=None, laboratorio=None, tipo_produto=None, 
                    com_cap=None, aliquota=None, preco_maximo=None, ordenar_por='produto'):
    """
    Busca produtos por critérios com filtros condicionais
    
    Args:
        connection: Conexão SQLite
        substancia: Nome da substância (busca parcial)
        laboratorio: Nome do laboratório (busca parcial)
        tipo_produto: Tipo de produto exato
        com_cap: Boolean - True para produtos com CAP, False para sem CAP
        aliquota: Valor da alíquota ICMS
        preco_maximo: Preço máximo de referência
        ordenar_por: 'preco', 'produto' ou 'laboratorio'
    
    Returns:
        list: Lista de produtos encontrados
    """
    cursor = connection.cursor()
    
    query = """
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
        WHERE 1=1
    """
    
    params = []
    
    if substancia:
        query += " AND s.nome_substancia LIKE ?"
        params.append(f'%{substancia}%')
    
    if laboratorio:
        query += " AND l.nome_laboratorio LIKE ?"
        params.append(f'%{laboratorio}%')
    
    if tipo_produto:
        query += " AND tp.tipo_produto = ?"
        params.append(tipo_produto)
    
    if com_cap is not None:
        if com_cap:
            query += " AND p.cap = 'Sim'"
        else:
            query += " AND p.cap = 'Não'"
    
    if aliquota is not None:
        query += " AND a.aliquota = ?"
        params.append(float(aliquota))
    
    if preco_maximo is not None:
        query += """ AND (
            CASE 
                WHEN p.cap = 'Sim' AND pmvg.pmvg_com_impostos IS NOT NULL THEN pmvg.pmvg_com_impostos
                ELSE pf.pf_com_impostos
            END
        ) <= ?"""
        params.append(float(preco_maximo))
    
    # Ordenação
    if ordenar_por == 'preco':
        query += " ORDER BY preco_referencia"
    elif ordenar_por == 'laboratorio':
        query += " ORDER BY l.nome_laboratorio"
    else:
        query += " ORDER BY p.nome_produto"
    
    cursor.execute(query, params)
    
    # Retorna como lista de dicionários
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

