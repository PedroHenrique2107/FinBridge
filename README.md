# Finance JSON Ingestor (MySQL 5.7) — 1 staging + 1 tabela final

## Objetivo
Ler 3 arquivos JSON locais (sempre mesmo nome), extrair apenas os campos necessários, gerar **1 linha por Parcela × Categoria Financeira** (mesmo sem baixa), e atualizar o MySQL via **upsert incremental** (atualiza apenas se mudou usando `row_hash`).

Arquivos:
- `SI_DATACOMPETPARCELAS.json` (base: parcelas × categories)
- `SI_DATAPAGTO.json` (baixas agregadas + conta da baixa mais recente)
- `SI_EXTRATO_CLIENTE_HISTORICO.json` (fallback de baixa/cliente/documento e complemento por join)

## Arquitetura
- `finance_flat` (tabela final única para consulta/planilha)
- `stg_finance_unified` (única staging table)
  - Registros do tipo:
    - `BASE` (parcela × categoria) com `rate`, `originalAmount`, `balanceAmount`
    - `PAY` (parcela × categoria) com agregados de baixa (já rateados) e `numConta` da baixa mais recente
    - `EXT` (parcela, sem categoria) com agregados do extrato (não rateados)

O merge final:
- Parte de `BASE` (define o grão e as categorias)
- LEFT JOIN com `PAY` (preenche baixa e conta)
- LEFT JOIN com `EXT` (fallback quando não há PAY)
- Calcula `ValorOriginalRateado` e `SaldoAtual` usando `rate`
- Faz upsert em `finance_flat` comparando `row_hash`

## Setup
1. Crie `.env` a partir de `.env.example`
2. Rode os SQLs em `sql/` (na ordem):
   - `sql/001_tables.sql`
   - `sql/002_merge.sql`
3. Instale e rode:
   - `npm i`
   - `npm run dev`

## Docker
- Coloque os JSONs em `./data/` com os nomes esperados
- `docker compose up --build`

## Export CSV
```sql
SELECT *
FROM finance_flat
ORDER BY Codigoempresa, NumeroDoTitulo, NumeroDaParcela, CodigoDoCentroDeCusto, numPlanoFinanceiro;
```
