# Consultas em Álgebra Relacional

Este documento apresenta a representação em Álgebra Relacional das principais consultas do sistema de gestão de preços de medicamentos governamentais.

## Convenções de Notação

- **σ (sigma)**: Seleção (filtro condicional)
- **π (pi)**: Projeção (seleção de colunas)
- **⋈ (bowtie)**: Junção natural (join baseado em atributos comuns)
- **γ (gamma)**: Agregação (GROUP BY com funções agregadas)
- **ρ (rho)**: Renomeação de relação
- **∪ (union)**: União
- **∩ (intersection)**: Interseção
- **− (difference)**: Diferença

---

## Consulta 1: Produtos com CAP aplicável e seus preços

### Descrição
Encontra todos os produtos que possuem CAP (Coeficiente de Adequação de Preços) aplicável e retorna suas informações de preço PF (Preço Fábrica) e PMVG (Preço Máximo Venda ao Governo), considerando apenas os preços mais recentes por produto.

### Álgebra Relacional

```
CAP_PRODUTOS ← σ(CAP='Sim' AND comercializacao_2024='Sim')(PRODUTOS)

PRECOS_RECENTES_PF ← 
    π(id_produto, pf_sem_impostos)(
        γ(id_produto; MAX(data_vigencia) → data_max)(
            PRECOS_FABRICA
        ) ⋈ PRECOS_FABRICA
    )

PRECOS_RECENTES_PMVG ← 
    π(id_produto, pmvg_sem_impostos)(
        γ(id_produto; MAX(data_vigencia) → data_max)(
            PRECOS_PMVG
        ) ⋈ PRECOS_PMVG
    )

PRODUTOS_CAP_PRECOS ← 
    CAP_PRODUTOS 
    ⋈ (id_produto = id_produto) PRECOS_RECENTES_PF
    ⋈ (id_produto = id_produto) PRECOS_RECENTES_PMVG

RESULTADO ← π(
    codigo_ggrem, 
    nome_produto, 
    apresentacao,
    pf_sem_impostos,
    pmvg_sem_impostos,
    (pmvg_sem_impostos - pf_sem_impostos) * 100.0 / pf_sem_impostos → desconto_percentual
)(PRODUTOS_CAP_PRECOS)
```

### Notação Expandida

```
R1 = σ(CAP='Sim' AND comercializacao_2024='Sim')(PRODUTOS)
R2 = γ(id_produto; MAX(data_vigencia) → data_max)(PRECOS_FABRICA)
R3 = R2 ⋈ (id_produto = id_produto AND data_vigencia = data_max) PRECOS_FABRICA
R4 = π(id_produto, pf_sem_impostos)(R3)
R5 = γ(id_produto; MAX(data_vigencia) → data_max)(PRECOS_PMVG)
R6 = R5 ⋈ (id_produto = id_produto AND data_vigencia = data_max) PRECOS_PMVG
R7 = π(id_produto, pmvg_sem_impostos)(R6)
R8 = R1 ⋈ (id_produto = id_produto) R4
R9 = R8 ⋈ (id_produto = id_produto) R7
RESULTADO = π(codigo_ggrem, nome_produto, apresentacao, pf_sem_impostos, pmvg_sem_impostos, desconto_percentual)(R9)
```

### Operadores Utilizados
- **σ**: Seleção (filtro condicional)
- **⋈**: Junção natural (join)
- **π**: Projeção (seleção de colunas)
- **γ**: Agregação com MAX para obter preços mais recentes

---

## Consulta 2: Laboratórios com maior número de produtos por classe terapêutica

### Descrição
Identifica laboratórios que possuem produtos em múltiplas classes terapêuticas, contando quantos produtos cada laboratório possui em cada classe. Considera apenas produtos comercializados em 2024.

### Álgebra Relacional

```
PRODUTOS_2024 ← σ(comercializacao_2024='Sim')(PRODUTOS)

PRODUTOS_CLASSES ← PRODUTOS_2024 ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS

PRODUTOS_LABORATORIOS ← PRODUTOS_CLASSES ⋈ (id_laboratorio = id_laboratorio) LABORATORIOS

AGRUPADO ← γ(
    id_laboratorio, 
    id_classe; 
    COUNT(id_produto) → total_produtos
)(PRODUTOS_LABORATORIOS)

RESULTADO ← π(
    nome_laboratorio,
    descricao_classe,
    total_produtos
)(
    AGRUPADO ⋈ (id_laboratorio = id_laboratorio) LABORATORIOS 
    ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS
)
ORDER BY total_produtos DESC
```

### Notação Expandida

```
R1 = σ(comercializacao_2024='Sim')(PRODUTOS)
R2 = R1 ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS
R3 = R2 ⋈ (id_laboratorio = id_laboratorio) LABORATORIOS
R4 = γ(id_laboratorio, id_classe; COUNT(id_produto) → total_produtos)(R3)
R5 = R4 ⋈ (id_laboratorio = id_laboratorio) LABORATORIOS
R6 = R5 ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS
RESULTADO = π(nome_laboratorio, descricao_classe, total_produtos)(R6)
```

### Operadores Utilizados
- **σ**: Seleção condicional
- **⋈**: Junção natural
- **γ**: Agregação com COUNT
- **π**: Projeção
- **ORDER BY**: Ordenação (extensão da álgebra relacional)

---

## Consulta 3: Produtos com preço acima da média da sua classe terapêutica

### Descrição
Identifica produtos cujo preço está acima da média de preços da sua respectiva classe terapêutica. Utiliza apenas preços mais recentes por produto para evitar duplicatas de histórico.

### Álgebra Relacional

```
PRECOS_RECENTES ← 
    π(id_produto, id_classe, pf_sem_impostos)(
        PRODUTOS 
        ⋈ (id_produto = id_produto) 
        π(id_produto, pf_sem_impostos)(
            γ(id_produto; MAX(data_vigencia) → data_max)(PRECOS_FABRICA) 
            ⋈ PRECOS_FABRICA
        )
    )

PRECO_POR_CLASSE ← γ(
    id_classe; 
    AVG(pf_sem_impostos) → preco_medio_classe
)(PRECOS_RECENTES)

PRODUTOS_COM_MEDIA ← PRECOS_RECENTES ⋈ (id_classe = id_classe) PRECO_POR_CLASSE

PRODUTOS_ACIMA_MEDIA ← σ(pf_sem_impostos > preco_medio_classe)(PRODUTOS_COM_MEDIA)

RESULTADO ← π(
    nome_produto,
    apresentacao,
    descricao_classe,
    pf_sem_impostos,
    preco_medio_classe,
    (pf_sem_impostos - preco_medio_classe) * 100.0 / preco_medio_classe → pct_acima_media
)(
    PRODUTOS_ACIMA_MEDIA ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS
)
```

### Notação Expandida

```
R1 = γ(id_produto; MAX(data_vigencia) → data_max)(PRECOS_FABRICA)
R2 = R1 ⋈ (id_produto = id_produto AND data_vigencia = data_max) PRECOS_FABRICA
R3 = π(id_produto, pf_sem_impostos)(R2)
R4 = PRODUTOS ⋈ (id_produto = id_produto) R3
R5 = π(id_produto, id_classe, pf_sem_impostos)(R4)
R6 = γ(id_classe; AVG(pf_sem_impostos) → preco_medio_classe)(R5)
R7 = R5 ⋈ (id_classe = id_classe) R6
R8 = σ(pf_sem_impostos > preco_medio_classe)(R7)
R9 = R8 ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS
RESULTADO = π(nome_produto, apresentacao, descricao_classe, pf_sem_impostos, preco_medio_classe, pct_acima_media)(R9)
```

### Operadores Utilizados
- **σ**: Seleção condicional com operador de comparação (>)
- **⋈**: Junção natural
- **γ**: Agregação com função AVG
- **π**: Projeção
- **MAX**: Função de agregação para obter preços mais recentes

---

## Consulta 4: Substâncias com maior variação de preço entre laboratórios

### Descrição
Identifica substâncias ativas com maior variação percentual de preço entre diferentes laboratórios, útil para análise regulatória e detecção de possíveis abusos de preço.

### Álgebra Relacional

```
PRECOS_RECENTES ← 
    π(id_produto, id_substancia, id_laboratorio, id_tipo, pf_sem_impostos)(
        PRODUTOS 
        ⋈ (id_produto = id_produto) 
        π(id_produto, pf_sem_impostos)(
            γ(id_produto; MAX(data_vigencia) → data_max)(PRECOS_FABRICA) 
            ⋈ PRECOS_FABRICA
        )
    )

PRODUTOS_VALIDOS ← σ(comercializacao_2024='Sim' AND tipo_produto != '-')(PRECOS_RECENTES)

ESTATISTICAS_SUBSTANCIA ← γ(
    id_substancia;
    COUNT(DISTINCT id_laboratorio) → qtd_laboratorios,
    COUNT(DISTINCT id_produto) → qtd_produtos,
    COUNT(DISTINCT id_tipo) → qtd_tipos,
    MIN(pf_sem_impostos) → preco_minimo,
    MAX(pf_sem_impostos) → preco_maximo,
    AVG(pf_sem_impostos) → preco_medio
)(PRODUTOS_VALIDOS)

SUBSTANCIAS_MULTIPLOS_LABS ← σ(qtd_laboratorios > 1)(ESTATISTICAS_SUBSTANCIA)

RESULTADO ← π(
    nome_substancia,
    qtd_laboratorios,
    qtd_produtos,
    qtd_tipos,
    preco_minimo,
    preco_maximo,
    preco_medio,
    (preco_maximo - preco_minimo) * 100.0 / preco_minimo → variacao_percentual
)(
    SUBSTANCIAS_MULTIPLOS_LABS ⋈ (id_substancia = id_substancia) SUBSTANCIAS
)
ORDER BY variacao_percentual DESC
```

### Operadores Utilizados
- **σ**: Seleção condicional múltipla
- **⋈**: Junção natural
- **γ**: Agregação com múltiplas funções (COUNT, MIN, MAX, AVG)
- **π**: Projeção com expressões calculadas
- **DISTINCT**: Eliminação de duplicatas na agregação

---

## Observações sobre os Operadores

### Operadores Fundamentais

1. **σ (Seleção)**: Filtra tuplas baseado em uma condição booleana
   - Exemplo: `σ(preco > 100)(PRODUTOS)` seleciona produtos com preço maior que 100

2. **π (Projeção)**: Seleciona apenas os atributos especificados
   - Exemplo: `π(nome, preco)(PRODUTOS)` retorna apenas nome e preço

3. **⋈ (Join Natural)**: Combina tuplas de duas relações baseado em atributos comuns
   - Exemplo: `PRODUTOS ⋈ PRECOS_FABRICA` junta por `id_produto`

### Operadores de Agregação

4. **γ (Agregação)**: Agrupa tuplas e aplica funções agregadas
   - Funções comuns: COUNT, SUM, AVG, MIN, MAX
   - Sintaxe: `γ(atributo_grupo; FUNCAO(atributo) → nome_resultado)(RELACAO)`

### Operadores de Conjunto

5. **∪ (União)**: Combina tuplas de duas relações compatíveis
6. **∩ (Interseção)**: Retorna tuplas presentes em ambas relações
7. **− (Diferença)**: Retorna tuplas da primeira relação não presentes na segunda

### Extensões Práticas

8. **ORDER BY**: Ordenação de resultados (extensão comum)
9. **DISTINCT**: Eliminação de duplicatas
10. **Expressões Calculadas**: Operações aritméticas em projeções

---

## Notas Importantes

- **Preços SEM impostos**: Todas as consultas utilizam `pf_sem_impostos` e `pmvg_sem_impostos` conforme o modelo atual do sistema
- **Preços mais recentes**: Consultas que envolvem preços utilizam `MAX(data_vigencia)` para evitar duplicatas de histórico
- **Filtros de qualidade**: Consultas aplicam filtros como `comercializacao_2024='Sim'` e `tipo_produto != '-'` para garantir dados válidos
- **Normalização**: O modelo segue princípios de normalização relacional, evitando redundâncias

Estas consultas demonstram operações fundamentais da Álgebra Relacional aplicadas ao contexto de gestão de preços de medicamentos governamentais, com foco em análise regulatória e otimização de compras públicas.
