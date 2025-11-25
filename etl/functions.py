"""
Funções auxiliares para interação com banco de dados PostgreSQL

Módulo contém funções para:
- atualizar_preco_produto: Atualiza preços com validações condicionais
- buscar_produtos: Busca flexível com múltiplos filtros e cálculo de preços finais

Todas as funções trabalham com preços SEM impostos como valores base.
"""
from datetime import date
from psycopg2.extras import RealDictCursor


def atualizar_preco_produto(
    connection, codigo_ggrem, id_aliquota, tipo_preco, novo_valor, usuario
):
    """
    Atualiza preço de um produto com validações condicionais.

    Validações implementadas:
    - Verifica existência do produto pelo código GGREM
    - PMVG sem CAP: não pode exceder PF (regra de negócio)
    - PF: alerta variações >50% (não bloqueia, apenas avisa)
    - Registra histórico automaticamente se houver mudança

    Args:
        connection: Conexão PostgreSQL (deve estar ativa)
        codigo_ggrem: Código GGREM único do produto (VARCHAR(20))
        id_aliquota: ID da alíquota ICMS (INTEGER, pode ser None para sem impostos)
        tipo_preco: Tipo de preço a atualizar ('PF' ou 'PMVG')
        novo_valor: Novo valor do preço (DECIMAL, deve ser positivo)
        usuario: Nome do usuário que fez a alteração (para auditoria)

    Returns:
        str: Mensagem de resultado da operação
            - "SUCESSO: ..." se atualização bem-sucedida
            - "AVISO: ..." se variação >50% detectada (mas prossegue)
            - "ERRO: ..." se validação falhar ou produto não encontrado

    Raises:
        psycopg2.Error: Se houver erro de conexão ou SQL
    """
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    try:
        # Obtém informações do produto
        cursor.execute(
            """
            SELECT p.id_produto, p.cap, rp.regime_preco 
            FROM produtos p
            INNER JOIN regimes_preco rp ON p.id_regime = rp.id_regime
            WHERE p.codigo_ggrem = %s
        """,
            (codigo_ggrem,),
        )

        produto = cursor.fetchone()
        if not produto:
            return "ERRO: Produto não encontrado"

        id_produto = produto["id_produto"]
        cap = produto["cap"]

        valor_anterior = None

        tabela_preco = "precos_pmvg" if tipo_preco == "PMVG" else "precos_fabrica"
        campo_preco = "pmvg_sem_impostos" if tipo_preco == "PMVG" else "pf_sem_impostos"
        
        if tipo_preco not in ("PF", "PMVG"):
            return "ERRO: Tipo de preço inválido"

        # Validações específicas
        if tipo_preco == "PMVG" and cap == "Não":
            # PMVG sem CAP não pode exceder PF
            cursor.execute(
                """
                SELECT pf_sem_impostos 
                FROM precos_fabrica
                WHERE id_produto = %s AND id_aliquota IS NOT DISTINCT FROM %s
                LIMIT 1
            """,
                (id_produto, id_aliquota),
            )
            pf_result = cursor.fetchone()
            if pf_result and pf_result["pf_sem_impostos"]:
                if novo_valor > pf_result["pf_sem_impostos"]:
                    return f"ERRO: PMVG ({novo_valor}) não pode ser maior que PF ({pf_result['pf_sem_impostos']}) para produtos sem CAP"

        # Busca valor anterior
        cursor.execute(
            f"""
            SELECT {campo_preco} 
            FROM {tabela_preco}
            WHERE id_produto = %s AND id_aliquota IS NOT DISTINCT FROM %s
            LIMIT 1
        """,
            (id_produto, id_aliquota),
        )
        resultado_anterior = cursor.fetchone()
        if resultado_anterior and resultado_anterior[campo_preco]:
            valor_anterior = resultado_anterior[campo_preco]
            # Valida variação percentual para PF
            if tipo_preco == "PF":
                variacao = abs((novo_valor - valor_anterior) / valor_anterior * 100)
                if variacao > 50:
                    print(f"AVISO: Variação de {variacao:.2f}% detectada. Prosseguindo com atualização.")

        # Atualiza ou insere usando ON CONFLICT
        cursor.execute(
            f"""
            INSERT INTO {tabela_preco} 
                (id_produto, id_aliquota, {campo_preco}, data_vigencia)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id_produto, id_aliquota, data_vigencia)
            DO UPDATE SET {campo_preco} = EXCLUDED.{campo_preco}
        """,
            (id_produto, id_aliquota, novo_valor, date.today()),
        )

        # Registra no histórico se houve mudança
        if valor_anterior is None or valor_anterior != novo_valor:
            cursor.execute(
                """
                INSERT INTO historico_precos 
                    (id_produto, tipo_preco, id_aliquota, valor_anterior, valor_novo, usuario_alteracao)
                VALUES (%s, %s::tipo_preco, %s, %s, %s, %s)
            """,
                (
                    id_produto,
                    tipo_preco,
                    id_aliquota,
                    valor_anterior,
                    novo_valor,
                    usuario,
                ),
            )

        connection.commit()

        valor_anterior_str = (
            str(valor_anterior) if valor_anterior is not None else "N/A"
        )
        return f"SUCESSO: Preço atualizado. Valor anterior: {valor_anterior_str}, Novo valor: {novo_valor}"

    except Exception as e:
        connection.rollback()
        return f"ERRO: {str(e)}"


def buscar_produtos(
    connection,
    substancia=None,
    laboratorio=None,
    tipo_produto=None,
    com_cap=None,
    aliquota=None,
    preco_maximo=None,
    ordenar_por="produto",
    canal="pf",
    aliquota_percent=None,
):
    """
    Busca produtos por critérios com filtros condicionais.

    Características:
    - Trabalha com preços SEM impostos como valores base
    - Calcula preço final aplicando ICMS se aliquota_percent fornecido
    - Respeita regra CAP: usa PMVG se disponível para canal governo
    - Suporta múltiplos filtros combinados (AND lógico)
    - Busca parcial em substância e laboratório (LIKE %valor%)

    Preço de Referência:
    - Canal 'governo': PMVG se CAP='Sim' e PMVG disponível, senão PF
    - Canal 'pf' (varejo): Sempre PF

    Preço Final:
    - Se aliquota_percent fornecido: preco_referencia * (1 + aliquota_percent/100)
    - Exceto produtos com icms_zero='Sim' (mantém preço sem ICMS)
    - Se aliquota_percent None: retorna apenas preco_referencia

    Args:
        connection: Conexão PostgreSQL (deve estar ativa)
        substancia: Nome da substância (str, busca parcial com LIKE)
        laboratorio: Nome do laboratório (str, busca parcial com LIKE)
        tipo_produto: Tipo de produto exato (str, ex: 'Genérico', 'Similar')
        com_cap: Boolean - True para produtos com CAP, False para sem CAP, None para ambos
        aliquota: Valor da alíquota ICMS (float, para filtrar por alíquota específica)
        preco_maximo: Preço máximo de referência (float, filtra pelo preço final se aliquota_percent fornecido)
        ordenar_por: Campo de ordenação ('preco', 'produto' ou 'laboratorio')
        canal: Canal de venda ('pf' para varejo, 'governo' para compra pública)
        aliquota_percent: Percentual ICMS efetivo do estado (float, 0-100). Se None, não aplica ICMS.

    Returns:
        list: Lista de dicionários com produtos encontrados. Cada dicionário contém:
            - codigo_ggrem, nome_produto, apresentacao
            - nome_substancia, nome_laboratorio, tipo_produto, regime_preco
            - cap, icms_zero, comercializacao_2024
            - aliquota, preco_fabrica, preco_pmvg
            - preco_referencia (PF ou PMVG conforme canal)
            - preco_final (com ICMS se aliquota_percent fornecido)

    Note:
        - Retorna apenas produtos com preços cadastrados (LEFT JOIN com precos_fabrica)
        - Usa DISTINCT para evitar duplicatas de múltiplos preços por produto
        - Considera apenas preços com id_aliquota IS NULL (sem impostos)
    """
    cursor = connection.cursor(cursor_factory=RealDictCursor)

    # Expressão de preço de referência (sempre SEM impostos)
    if canal and canal.lower() == "governo":
        preco_ref_expr = """
            CASE 
                WHEN p.cap = 'Sim' AND pmvg.pmvg_sem_impostos IS NOT NULL THEN pmvg.pmvg_sem_impostos
                ELSE pf.pf_sem_impostos
            END
        """
    else:
        # Varejo: usa PF sempre
        preco_ref_expr = "pf.pf_sem_impostos"

    # Preço final no estado (aplica aliquota_percent se fornecida, exceto ICMS zero)
    # Usa o valor diretamente na expressão SQL já que é um parâmetro da função (não entrada do usuário)
    if aliquota_percent is not None:
        aliquota_value = float(aliquota_percent)
        preco_final_expr = f"CASE WHEN p.icms_zero = 'Sim' THEN ({preco_ref_expr}) ELSE ({preco_ref_expr}) * (1 + {aliquota_value}/100.0) END"
    else:
        preco_final_expr = f"({preco_ref_expr})"

    query = f"""
        SELECT DISTINCT
            p.codigo_ggrem,
            p.nome_produto,
            p.apresentacao,
            s.nome_substancia,
            l.nome_laboratorio,
            tp.tipo_produto,
            rp.regime_preco,
            p.cap,
            p.icms_zero,
            p.comercializacao_2024,
            a.aliquota,
            pf.pf_sem_impostos AS preco_fabrica,
            pmvg.pmvg_sem_impostos AS preco_pmvg,
            {preco_ref_expr} AS preco_referencia,
            {preco_final_expr} AS preco_final
        FROM produtos p
        INNER JOIN substancias s ON p.id_substancia = s.id_substancia
        INNER JOIN laboratorios l ON p.id_laboratorio = l.id_laboratorio
        INNER JOIN tipos_produto tp ON p.id_tipo = tp.id_tipo
        INNER JOIN regimes_preco rp ON p.id_regime = rp.id_regime
        LEFT JOIN precos_fabrica pf ON p.id_produto = pf.id_produto AND pf.id_aliquota IS NULL
        LEFT JOIN precos_pmvg pmvg ON p.id_produto = pmvg.id_produto AND pmvg.id_aliquota IS NULL
        LEFT JOIN aliquotas_icms a ON pf.id_aliquota = a.id_aliquota
        WHERE 1=1
    """

    params = []

    if substancia:
        query += " AND s.nome_substancia LIKE %s"
        params.append(f"%{substancia}%")

    if laboratorio:
        query += " AND l.nome_laboratorio LIKE %s"
        params.append(f"%{laboratorio}%")

    if tipo_produto:
        query += " AND tp.tipo_produto = %s"
        params.append(tipo_produto)

    if com_cap is not None:
        if com_cap:
            query += " AND p.cap = 'Sim'"
        else:
            query += " AND p.cap = 'Não'"

    if aliquota is not None:
        query += " AND a.aliquota = %s"
        params.append(float(aliquota))

    if preco_maximo is not None:
        # Filtra pelo preço final se aliquota_percent foi fornecida; caso contrário, pelo preço de referência
        if aliquota_percent is not None:
            query += " AND (preco_final) <= %s"
        else:
            query += " AND (preco_referencia) <= %s"
        params.append(float(preco_maximo))

    # Ordenação
    if ordenar_por == "preco":
        # Ordena por preço final quando disponível, senão por referência
        if aliquota_percent is not None:
            query += " ORDER BY preco_final"
        else:
            query += " ORDER BY preco_referencia"
    elif ordenar_por == "laboratorio":
        query += " ORDER BY l.nome_laboratorio"
    else:
        query += " ORDER BY p.nome_produto"

    cursor.execute(query, params)

    # Retorna como lista de dicionários (RealDictCursor já retorna dicts)
    return cursor.fetchall()
