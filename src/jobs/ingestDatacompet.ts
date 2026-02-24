import fs from "node:fs";
import { chain } from "stream-chain";
import { parser } from "stream-json";
import { pick } from "stream-json/filters/Pick";
import { streamArray } from "stream-json/streamers/StreamArray";
import type { PoolConnection } from "mysql2/promise";
import { bulkInsert } from "../db/bulkInsert.js";

export async function ingestDatacompet(
  conn: PoolConnection,
  runId: string,
  filePath: string,
  batchSize: number
): Promise<void> {
  const columns = [
    "run_id","record_type",
    "Codigoempresa","NumeroDoTitulo","NumeroDaParcela",
    "CodigoDoCentroDeCusto","NomeDoCentroDeCusto",
    "numPlanoFinanceiro","PlanoFinanceiro","rate",
    "NomeDaEmpresa",
    "CodigoDoCliente","NomeDoCliente","NumeroCPFCNPJ",
    "NumeroDoDocumento","NomeDoDocumento",
    "NomeDoTipoDeCondicao",
    "DataDeEmissao","DataDeVencimento",
    "originalAmount","balanceAmount"
  ];

  let batch: any[] = [];

  const pipeline = chain([
    fs.createReadStream(filePath),
    parser(),
    pick({ filter: "data" }),
    streamArray(),
  ]);

  for await (const { value } of pipeline as any) {
    const base = {
      run_id: runId,
      record_type: "BASE",
      Codigoempresa: value.companyId ?? null,
      NomeDaEmpresa: value.companyName ?? null,
      CodigoDoCliente: value.clientId ?? null,
      NomeDoCliente: value.clientName ?? null,
      NumeroCPFCNPJ: value.clientDocumentNumber ?? null,
      NumeroDoDocumento: value.documentNumber ?? null,
      NomeDoDocumento: value.documentIdentificationName ?? null,
      NumeroDoTitulo: value.billId ?? null,
      NumeroDaParcela: value.installmentId ?? null,
      NomeDoTipoDeCondicao: value.conditionTypeName ?? null,
      DataDeEmissao: value.issueDate ?? null,
      DataDeVencimento: value.dueDate ?? null,
      originalAmount: value.originalAmount ?? null,
      balanceAmount: value.balanceAmount ?? null,
    };

    const cats = Array.isArray(value.receiptsCategories) ? value.receiptsCategories : [];
    for (const c of cats) {
      batch.push({
        ...base,
        CodigoDoCentroDeCusto: c.costCenterId ?? null,
        NomeDoCentroDeCusto: c.costCenterName ?? null,
        numPlanoFinanceiro: c.financialCategoryId ?? null,
        PlanoFinanceiro: c.financialCategoryName ?? null,
        rate: c.financialCategoryRate ?? null,
      });

      if (batch.length >= batchSize) {
        await bulkInsert(conn, "stg_finance_unified", columns, batch);
        batch = [];
      }
    }
  }

  if (batch.length) await bulkInsert(conn, "stg_finance_unified", columns, batch);
}
