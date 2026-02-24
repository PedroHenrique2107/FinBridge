-- MySQL 5.7

CREATE TABLE IF NOT EXISTS finance_flat (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

  row_key  BINARY(32) NOT NULL,
  row_hash BINARY(32) NOT NULL,

  Codigoempresa BIGINT NULL,
  NomeDaEmpresa VARCHAR(255) NULL,

  CodigoDoCentroDeCusto BIGINT NULL,
  NomeDoCentroDeCusto VARCHAR(255) NULL,

  numPlanoFinanceiro VARCHAR(32) NULL,
  PlanoFinanceiro VARCHAR(255) NULL,

  CodigoDoCliente BIGINT NULL,
  NomeDoCliente VARCHAR(255) NULL,
  NumeroCPFCNPJ VARCHAR(32) NULL,

  NumeroDoDocumento VARCHAR(64) NULL,
  NomeDoDocumento VARCHAR(255) NULL,

  NumeroDoTitulo BIGINT NULL,
  NumeroDaParcela BIGINT NULL,

  NomeDoTipoDeCondicao VARCHAR(255) NULL,

  DataDeEmissao DATE NULL,
  DataDeVencimento DATE NULL,

  ValorOriginalRateado DECIMAL(13,2) NULL,
  SaldoAtual DECIMAL(13,2) NULL,

  ValorDaBaixaRateado DECIMAL(13,2) NULL,
  Datadabaixa DATE NULL,

  AcrescimoRateado DECIMAL(13,2) NULL,
  DescontoRateado DECIMAL(13,2) NULL,
  ValorLiquido DECIMAL(13,2) NULL,

  numConta VARCHAR(64) NULL,

  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  UNIQUE KEY uk_row_key (row_key),

  KEY idx_empresa_titulo_parcela (Codigoempresa, NumeroDoTitulo, NumeroDaParcela),
  KEY idx_documento (NumeroDoDocumento),
  KEY idx_cliente (CodigoDoCliente),
  KEY idx_vencimento (DataDeVencimento),
  KEY idx_baixa (Datadabaixa)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stg_finance_unified (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  run_id CHAR(36) NOT NULL,
  record_type ENUM('BASE','PAY','EXT') NOT NULL,

  Codigoempresa BIGINT NULL,
  NumeroDoTitulo BIGINT NULL,
  NumeroDaParcela BIGINT NULL,

  CodigoDoCentroDeCusto BIGINT NULL,
  NomeDoCentroDeCusto VARCHAR(255) NULL,
  numPlanoFinanceiro VARCHAR(32) NULL,
  PlanoFinanceiro VARCHAR(255) NULL,
  rate DECIMAL(9,4) NULL,

  NomeDaEmpresa VARCHAR(255) NULL,

  CodigoDoCliente BIGINT NULL,
  NomeDoCliente VARCHAR(255) NULL,
  NumeroCPFCNPJ VARCHAR(32) NULL,

  NumeroDoDocumento VARCHAR(64) NULL,
  NomeDoDocumento VARCHAR(255) NULL,

  NomeDoTipoDeCondicao VARCHAR(255) NULL,

  DataDeEmissao DATE NULL,
  DataDeVencimento DATE NULL,

  originalAmount DECIMAL(13,2) NULL,
  balanceAmount DECIMAL(13,2) NULL,

  ValorDaBaixaRateado DECIMAL(13,2) NULL,
  Datadabaixa DATE NULL,
  AcrescimoRateado DECIMAL(13,2) NULL,
  DescontoRateado DECIMAL(13,2) NULL,
  ValorLiquido DECIMAL(13,2) NULL,
  numConta VARCHAR(64) NULL,

  ValorLiquidoBase DECIMAL(13,2) NULL,
  DatadabaixaExt DATE NULL,

  inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  PRIMARY KEY (id),
  KEY idx_run (run_id),
  KEY idx_key (Codigoempresa, NumeroDoTitulo, NumeroDaParcela),
  KEY idx_key_cat (Codigoempresa, NumeroDoTitulo, NumeroDaParcela, CodigoDoCentroDeCusto, numPlanoFinanceiro),
  KEY idx_type (record_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
