import fs from "node:fs";
import { chain } from "stream-chain";
import { parser } from "stream-json";
import { pick } from "stream-json/filters/Pick";
import { streamArray } from "stream-json/streamers/StreamArray";
import type { PoolConnection } from "mysql2/promise";
import { bulkInsert } from "../db/bulkInsert.js";

export async function ingestExtrato(
  conn: PoolConnection,
  runId: string,
  filePath: string,
  batchSize: number
): Promise<void> {
  const columns = [
    "run_id","record_type",
    "Codigoempresa","NumeroDoTitulo","NumeroDaParcela",
    "CodigoDoCliente","NomeDoCliente","NumeroCPFCNPJ",
    "CodigoDoCentroDeCusto","NomeDoCentroDeCusto",
    "NumeroDoDocumento","NomeDoDocumento",
    "DataDeEmissao","DataDeVencimento",
    "DatadabaixaExt","ValorLiquidoBase",
    "NomeDaEmpresa"
  ];

  let batch: any[] = [];

  const pipeline = chain([
    fs.createReadStream(filePath),
    parser(),
    pick({ filter: "data" }),
    streamArray(),
  ]);

  for await (const { value } of pipeline as any) {
    const companyId = value.company?.id ?? null;
    const companyName = value.company?.name ?? null;
    const billId = value.billReceivableId ?? null;

    const customerId = value.customer?.id ?? null;
    const customerName = value.customer?.name ?? null;
    const customerDoc = value.customer?.document ?? null;

    const ccId = value.costCenter?.id ?? null;
    const ccName = value.costCenter?.name ?? null;

    const docNumber = value.document ?? null;
    const emissionDate = value.emissionDate ?? null;

    const installments = Array.isArray(value.installments) ? value.installments : [];
    for (const inst of installments) {
      const instId = inst.id ?? null;
      const dueDate = inst.dueDate ?? null;

      const receipts = Array.isArray(inst.receipts) ? inst.receipts : [];
      let maxDate: string | null = null;
      let sumNet = 0;

      for (const r of receipts) {
        const dt = (r.date ?? null) as string | null;
        const net = Number(r.netReceipt ?? 0);
        sumNet += net;
        if (dt && (!maxDate || dt > maxDate)) maxDate = dt;
      }

      batch.push({
        run_id: runId,
        record_type: "EXT",
        Codigoempresa: companyId,
        NumeroDoTitulo: billId,
        NumeroDaParcela: instId,
        CodigoDoCliente: customerId,
        NomeDoCliente: customerName,
        NumeroCPFCNPJ: customerDoc,
        CodigoDoCentroDeCusto: ccId,
        NomeDoCentroDeCusto: ccName,
        NumeroDoDocumento: docNumber,
        NomeDoDocumento: null,
        DataDeEmissao: emissionDate,
        DataDeVencimento: dueDate,
        DatadabaixaExt: maxDate,
        ValorLiquidoBase: Number(sumNet.toFixed(2)),
        NomeDaEmpresa: companyName,
      });

      if (batch.length >= batchSize) {
        await bulkInsert(conn, "stg_finance_unified", columns, batch);
        batch = [];
      }
    }
  }

  if (batch.length) await bulkInsert(conn, "stg_finance_unified", columns, batch);
}
