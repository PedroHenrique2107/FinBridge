-- Passe o run_id como parâmetro (?) via aplicação

INSERT INTO finance_flat (
  row_key, row_hash,
  Codigoempresa, NomeDaEmpresa,
  CodigoDoCentroDeCusto, NomeDoCentroDeCusto,
  numPlanoFinanceiro, PlanoFinanceiro,
  CodigoDoCliente, NomeDoCliente, NumeroCPFCNPJ,
  NumeroDoDocumento, NomeDoDocumento,
  NumeroDoTitulo, NumeroDaParcela,
  NomeDoTipoDeCondicao,
  DataDeEmissao, DataDeVencimento,
  ValorOriginalRateado, SaldoAtual,
  ValorDaBaixaRateado, Datadabaixa,
  AcrescimoRateado, DescontoRateado, ValorLiquido,
  numConta
)
SELECT
  UNHEX(SHA2(CONCAT_WS('|',
    b.Codigoempresa, b.NumeroDoTitulo, b.NumeroDaParcela,
    IFNULL(b.CodigoDoCentroDeCusto,''), IFNULL(b.numPlanoFinanceiro,'')
  ), 256)) AS row_key,

  UNHEX(SHA2(CONCAT_WS('|',
    b.Codigoempresa, IFNULL(b.NomeDaEmpresa,''),
    IFNULL(b.CodigoDoCentroDeCusto,''), IFNULL(b.NomeDoCentroDeCusto,''),
    IFNULL(b.numPlanoFinanceiro,''), IFNULL(b.PlanoFinanceiro,''),
    IFNULL(COALESCE(b.CodigoDoCliente, e.CodigoDoCliente),''), IFNULL(COALESCE(b.NomeDoCliente, e.NomeDoCliente),''),
    IFNULL(COALESCE(b.NumeroCPFCNPJ, e.NumeroCPFCNPJ),''),
    IFNULL(COALESCE(b.NumeroDoDocumento, e.NumeroDoDocumento),''), IFNULL(COALESCE(b.NomeDoDocumento, e.NomeDoDocumento),''),
    IFNULL(b.NumeroDoTitulo,''), IFNULL(b.NumeroDaParcela,''),
    IFNULL(b.NomeDoTipoDeCondicao,''),
    IFNULL(COALESCE(b.DataDeEmissao, e.DataDeEmissao),''), IFNULL(COALESCE(b.DataDeVencimento, e.DataDeVencimento),''),

    ROUND(IFNULL(b.originalAmount,0) * IFNULL(b.rate,0) / 100, 2),
    ROUND(IFNULL(b.balanceAmount,0)  * IFNULL(b.rate,0) / 100, 2),

    ROUND(IFNULL(p.ValorDaBaixaRateado, IFNULL(e.ValorLiquidoBase,0) * IFNULL(b.rate,0) / 100), 2),
    IFNULL(p.Datadabaixa, e.DatadabaixaExt),

    ROUND(IFNULL(p.AcrescimoRateado, 0), 2),
    ROUND(IFNULL(p.DescontoRateado, 0), 2),
    ROUND(IFNULL(p.ValorLiquido, IFNULL(e.ValorLiquidoBase,0) * IFNULL(b.rate,0) / 100), 2),

    IFNULL(p.numConta,'')
  ), 256)) AS row_hash,

  b.Codigoempresa, b.NomeDaEmpresa,
  b.CodigoDoCentroDeCusto, b.NomeDoCentroDeCusto,
  b.numPlanoFinanceiro, b.PlanoFinanceiro,
  COALESCE(b.CodigoDoCliente, e.CodigoDoCliente),
  COALESCE(b.NomeDoCliente,   e.NomeDoCliente),
  COALESCE(b.NumeroCPFCNPJ,   e.NumeroCPFCNPJ),
  COALESCE(b.NumeroDoDocumento, e.NumeroDoDocumento),
  COALESCE(b.NomeDoDocumento,   e.NomeDoDocumento),
  b.NumeroDoTitulo, b.NumeroDaParcela,
  b.NomeDoTipoDeCondicao,
  COALESCE(b.DataDeEmissao, e.DataDeEmissao),
  COALESCE(b.DataDeVencimento, e.DataDeVencimento),

  ROUND(IFNULL(b.originalAmount,0) * IFNULL(b.rate,0) / 100, 2),
  ROUND(IFNULL(b.balanceAmount,0)  * IFNULL(b.rate,0) / 100, 2),

  ROUND(IFNULL(p.ValorDaBaixaRateado, IFNULL(e.ValorLiquidoBase,0) * IFNULL(b.rate,0) / 100), 2),
  IFNULL(p.Datadabaixa, e.DatadabaixaExt),

  ROUND(IFNULL(p.AcrescimoRateado, 0), 2),
  ROUND(IFNULL(p.DescontoRateado, 0), 2),
  ROUND(IFNULL(p.ValorLiquido, IFNULL(e.ValorLiquidoBase,0) * IFNULL(b.rate,0) / 100), 2),

  p.numConta
FROM stg_finance_unified b
LEFT JOIN stg_finance_unified p
  ON p.run_id = b.run_id
 AND p.record_type = 'PAY'
 AND p.Codigoempresa = b.Codigoempresa
 AND p.NumeroDoTitulo = b.NumeroDoTitulo
 AND p.NumeroDaParcela = b.NumeroDaParcela
 AND IFNULL(p.CodigoDoCentroDeCusto,-1) = IFNULL(b.CodigoDoCentroDeCusto,-1)
 AND IFNULL(p.numPlanoFinanceiro,'') = IFNULL(b.numPlanoFinanceiro,'')

LEFT JOIN stg_finance_unified e
  ON e.run_id = b.run_id
 AND e.record_type = 'EXT'
 AND e.Codigoempresa = b.Codigoempresa
 AND e.NumeroDoTitulo = b.NumeroDoTitulo
 AND e.NumeroDaParcela = b.NumeroDaParcela

WHERE b.run_id = ?
  AND b.record_type = 'BASE'

ON DUPLICATE KEY UPDATE
  row_hash = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(row_hash), finance_flat.row_hash),

  Codigoempresa = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(Codigoempresa), Codigoempresa),
  NomeDaEmpresa = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NomeDaEmpresa), NomeDaEmpresa),

  CodigoDoCentroDeCusto = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(CodigoDoCentroDeCusto), CodigoDoCentroDeCusto),
  NomeDoCentroDeCusto   = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NomeDoCentroDeCusto), NomeDoCentroDeCusto),

  numPlanoFinanceiro = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(numPlanoFinanceiro), numPlanoFinanceiro),
  PlanoFinanceiro    = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(PlanoFinanceiro), PlanoFinanceiro),

  CodigoDoCliente = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(CodigoDoCliente), CodigoDoCliente),
  NomeDoCliente   = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NomeDoCliente), NomeDoCliente),
  NumeroCPFCNPJ   = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NumeroCPFCNPJ), NumeroCPFCNPJ),

  NumeroDoDocumento = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NumeroDoDocumento), NumeroDoDocumento),
  NomeDoDocumento   = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NomeDoDocumento), NomeDoDocumento),

  NumeroDoTitulo  = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NumeroDoTitulo), NumeroDoTitulo),
  NumeroDaParcela = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NumeroDaParcela), NumeroDaParcela),

  NomeDoTipoDeCondicao = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(NomeDoTipoDeCondicao), NomeDoTipoDeCondicao),

  DataDeEmissao    = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(DataDeEmissao), DataDeEmissao),
  DataDeVencimento = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(DataDeVencimento), DataDeVencimento),

  ValorOriginalRateado = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(ValorOriginalRateado), ValorOriginalRateado),
  SaldoAtual           = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(SaldoAtual), SaldoAtual),

  ValorDaBaixaRateado = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(ValorDaBaixaRateado), ValorDaBaixaRateado),
  Datadabaixa         = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(Datadabaixa), Datadabaixa),

  AcrescimoRateado = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(AcrescimoRateado), AcrescimoRateado),
  DescontoRateado  = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(DescontoRateado), DescontoRateado),
  ValorLiquido     = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(ValorLiquido), ValorLiquido),

  numConta = IF(finance_flat.row_hash <> VALUES(row_hash), VALUES(numConta), numConta),

  updated_at = IF(finance_flat.row_hash <> VALUES(row_hash), CURRENT_TIMESTAMP, updated_at);
