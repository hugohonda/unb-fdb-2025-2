# Introdução: Sistema de Gerenciamento de Preços de Medicamentos Governamentais

## Contexto do Problema

O sistema público de saúde brasileiro necessita realizar aquisições de medicamentos de forma eficiente e transparente, garantindo que os preços praticados estejam em conformidade com as regulamentações estabelecidas pela CMED (Câmara de Regulação do Mercado de Medicamentos). 

A lista de preços de medicamentos (TA_PRECO_MEDICAMENTO_GOV.csv) contém informações sobre:

- **Preço Fábrica (PF)**: Preço máximo que pode ser praticado por produtores, importadores e distribuidores para venda a farmácias e drogarias

- **Preço Máximo de Venda ao Governo (PMVG)**: Preço teto para vendas aos entes da administração pública, calculado aplicando o Coeficiente de Adequação de Preços (CAP) sobre o PF quando aplicável

## Desafios Enfrentados

1. **Volume de Dados**: A base contém milhares de registros com informações sobre múltiplas alíquotas de ICMS, diferentes apresentações de medicamentos e diversos laboratórios

2. **Complexidade Regulatória**: Diferentes regras aplicam-se conforme o tipo de medicamento (regulado, liberado), regime de preço, aplicação do CAP, e destino da comercialização

3. **Necessidade de Consultas Complexas**: Gestores públicos precisam realizar consultas que combinem múltiplos critérios (laboratório, substância, tipo de produto, alíquotas de ICMS, etc.)

4. **Integridade e Auditoria**: É necessário garantir rastreabilidade das alterações de preços e validações automáticas conforme regras de negócio

## Objetivo da Solução

Desenvolver um sistema de banco de dados relacional que permita:

- Armazenar e organizar os dados de preços de forma normalizada
- Realizar consultas complexas para análise de preços
- Implementar regras de negócio através de triggers e procedures
- Fornecer visões (views) que simplifiquem consultas frequentes
- Garantir integridade referencial e validação de dados
- Facilitar a importação e atualização dos dados através de processos ETL automatizados

Esta solução visa apoiar gestores públicos na tomada de decisão sobre aquisições de medicamentos, garantindo conformidade regulatória e otimização de recursos públicos.

