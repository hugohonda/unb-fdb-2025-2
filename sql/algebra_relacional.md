# Consultas em Álgebra Relacional

## Consulta 1: Produtos com CAP aplicável e seus preços

### Descrição
Encontra todos os produtos que possuem CAP (Coeficiente de Adequação de Preços) aplicável e retorna suas informações de preço PF e PMVG.

### Álgebra Relacional

```
CAP_PRODUTOS ← σ(CAP='Sim')(PRODUTOS)

PRODUTOS_CAP_PRECOS ← CAP_PRODUTOS ⋈ PRECOS_FABRICA ⋈ PRECOS_PMVG

RESULTADO ← π(
    codigo_ggrem, 
    nome_produto, 
    apresentacao,
    pf_com_impostos,
    pmvg_com_impostos
)(PRODUTOS_CAP_PRECOS)
```

### Notação Expandida

```
R1 = σ(CAP='Sim')(PRODUTOS)
R2 = R1 ⋈ (id_produto = id_produto) PRECOS_FABRICA
R3 = R2 ⋈ (id_produto = id_produto) PRECOS_PMVG
RESULTADO = π(codigo_ggrem, nome_produto, apresentacao, pf_com_impostos, pmvg_com_impostos)(R3)
```

### Operadores Utilizados
- **σ (sigma)**: Seleção (filtro condicional)
- **⋈ (bowtie)**: Junção natural (join)
- **π (pi)**: Projeção (seleção de colunas)

---

## Consulta 2: Laboratórios com maior número de produtos por classe terapêutica

### Descrição
Identifica laboratórios que possuem produtos em múltiplas classes terapêuticas, contando quantos produtos cada laboratório possui em cada classe.

### Álgebra Relacional

```
PRODUTOS_CLASSES ← PRODUTOS ⋈ CLASSES_TERAPEUTICAS

PRODUTOS_LABORATORIOS ← PRODUTOS_CLASSES ⋈ LABORATORIOS

AGRUPADO ← γ(
    id_laboratorio, 
    id_classe, 
    COUNT(id_produto) → total_produtos
)(PRODUTOS_LABORATORIOS)

RESULTADO ← π(
    nome_laboratorio,
    descricao_classe,
    total_produtos
)(
    AGRUPADO ⋈ LABORATORIOS ⋈ CLASSES_TERAPEUTICAS
)
```

### Notação Expandida

```
R1 = PRODUTOS ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS
R2 = R1 ⋈ (id_laboratorio = id_laboratorio) LABORATORIOS
R3 = γ(id_laboratorio, id_classe; COUNT(id_produto) → total_produtos)(R2)
R4 = R3 ⋈ LABORATORIOS ⋈ CLASSES_TERAPEUTICAS
RESULTADO = π(nome_laboratorio, descricao_classe, total_produtos)(R4)
```

### Operadores Utilizados
- **⋈**: Junção natural
- **γ (gamma)**: Agregação (GROUP BY)
- **π**: Projeção
- **COUNT**: Função de agregação

---

## Consulta 3: Produtos com preço acima da média da sua classe terapêutica

### Descrição
Identifica produtos cujo preço está acima da média de preços da sua respectiva classe terapêutica, utilizando divisão relacional para encontrar a média e depois comparar.

### Álgebra Relacional

```
PRODUTOS_PRECOS ← PRODUTOS ⋈ PRECOS_FABRICA

PRECO_POR_CLASSE ← γ(
    id_classe; 
    AVG(pf_com_impostos) → preco_medio_classe
)(PRODUTOS_PRECOS)

PRODUTOS_COM_MEDIA ← PRODUTOS_PRECOS ⋈ PRECO_POR_CLASSE

PRODUTOS_ACIMA_MEDIA ← σ(pf_com_impostos > preco_medio_classe)(PRODUTOS_COM_MEDIA)

RESULTADO ← π(
    nome_produto,
    apresentacao,
    descricao_classe,
    pf_com_impostos,
    preco_medio_classe
)(
    PRODUTOS_ACIMA_MEDIA ⋈ CLASSES_TERAPEUTICAS
)
```

### Notação Expandida

```
R1 = PRODUTOS ⋈ (id_produto = id_produto) PRECOS_FABRICA
R2 = γ(id_classe; AVG(pf_com_impostos) → preco_medio_classe)(R1)
R3 = R1 ⋈ (id_classe = id_classe) R2
R4 = σ(pf_com_impostos > preco_medio_classe)(R3)
R5 = R4 ⋈ (id_classe = id_classe) CLASSES_TERAPEUTICAS
RESULTADO = π(nome_produto, apresentacao, descricao_classe, pf_com_impostos, preco_medio_classe)(R5)
```

### Operadores Utilizados
- **σ**: Seleção condicional
- **⋈**: Junção natural
- **γ**: Agregação com função AVG
- **π**: Projeção
- **>**: Operador de comparação na condição de seleção

---

## Observações sobre os Operadores

1. **σ (Seleção)**: Filtra tuplas baseado em uma condição booleana
2. **π (Projeção)**: Seleciona apenas os atributos especificados
3. **⋈ (Join Natural)**: Combina tuplas de duas relações baseado em atributos comuns
4. **γ (Agregação)**: Agrupa tuplas e aplica funções agregadas (COUNT, AVG, SUM, etc.)
5. **Operadores de Comparação**: Utilizados dentro de σ para condições (>, <, =, ≠)

Estas consultas demonstram operações fundamentais da Álgebra Relacional aplicadas ao contexto de gestão de preços de medicamentos governamentais.

