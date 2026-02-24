import fs from "node:fs";
import { chain } from "stream-chain";
import { parser } from "stream-json";
import { pick } from "stream-json/filters/Pick";
import { streamArray } from "stream-json/streamers/StreamArray";
import type { PoolConnection } from "mysql2/promise";
import { bulkInsert } from "../db/bulkInsert.js";

type Agg = {
  Codigoempresa: number|null;
  NumeroDoTitulo: number|null;
  NumeroDaParcela: number|null;
  CodigoDoCentroDeCusto: number|null;
  NomeDoCentroDeCusto: string|null;
  numPlanoFinanceiro: string|null;
  PlanoFinanceiro: string|null;
  rate: number;

  ValorDaBaixaRateado: number;
  AcrescimoRateado: number;
  DescontoRateado: number;
  ValorLiquido: number;

  latestPaymentDate: string|null;
  Datadabaixa: string|null;
  numConta: string|null;
};

function makeKey(a: Agg) {
  return [
    a.Codigoempresa ?? "",
    a.NumeroDoTitulo ?? "",
    a.NumeroDaParcela ?? "",
    a.CodigoDoCentroDeCusto ?? "",
    a.numPlanoFinanceiro ?? "",
  ].join("|");
}

export async function ingestDatapagto(
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
    "ValorDaBaixaRateado","Datadabaixa",
    "AcrescimoRateado","DescontoRateado","ValorLiquido",
    "numConta"
  ];

  let batch: any[] = [];

  const pipeline = chain([
    fs.createReadStream(filePath),
    parser(),
    pick({ filter: "data" }),
    streamArray(),
  ]);

  for await (const { value } of pipeline as any) {
    const companyId = value.companyId ?? null;
    const billId = value.billId ?? null;
    const instId = value.installmentId ?? null;

    const receipts = Array.isArray(value.receipts) ? value.receipts : [];
    const map = new Map<string, Agg>();

    for (const r of receipts) {
      const paymentDate = (r.paymentDate ?? null) as string | null;
      const netAmount = Number(r.netAmount ?? 0);
      const addAmount = Number(r.additionAmount ?? 0);
      const discAmount = Number(r.discountAmount ?? 0);
      const accountNumber = (r.accountNumber ?? null) as string | null;

      const bms = Array.isArray(r.bankMovements) ? r.bankMovements : [];
      for (const bm of bms) {
        const fcs = Array.isArray(bm.financialCategories) ? bm.financialCategories : [];
        for (const fc of fcs) {
          const rate = Number(fc.financialCategoryRate ?? 0);

          const seed: Agg = {
            Codigoempresa: companyId,
            NumeroDoTitulo: billId,
            NumeroDaParcela: instId,
            CodigoDoCentroDeCusto: fc.costCenterId ?? null,
            NomeDoCentroDeCusto: fc.costCenterName ?? null,
            numPlanoFinanceiro: fc.financialCategoryId ?? null,
            PlanoFinanceiro: fc.financialCategoryName ?? null,
            rate,
            ValorDaBaixaRateado: 0,
            AcrescimoRateado: 0,
            DescontoRateado: 0,
            ValorLiquido: 0,
            latestPaymentDate: null,
            Datadabaixa: null,
            numConta: null,
          };

          const k = makeKey(seed);
          const cur = map.get(k) ?? seed;

          cur.ValorDaBaixaRateado += (netAmount * rate) / 100;
          cur.AcrescimoRateado += (addAmount * rate) / 100;
          cur.DescontoRateado += (discAmount * rate) / 100;
          cur.ValorLiquido += (netAmount * rate) / 100;

          if (paymentDate && (!cur.latestPaymentDate || paymentDate > cur.latestPaymentDate)) {
            cur.latestPaymentDate = paymentDate;
            cur.Datadabaixa = paymentDate;
            cur.numConta = accountNumber;
          }

          map.set(k, cur);
        }
      }
    }

    for (const cur of map.values()) {
      batch.push({
        run_id: runId,
        record_type: "PAY",
        Codigoempresa: cur.Codigoempresa,
        NumeroDoTitulo: cur.NumeroDoTitulo,
        NumeroDaParcela: cur.NumeroDaParcela,
        CodigoDoCentroDeCusto: cur.CodigoDoCentroDeCusto,
        NomeDoCentroDeCusto: cur.NomeDoCentroDeCusto,
        numPlanoFinanceiro: cur.numPlanoFinanceiro,
        PlanoFinanceiro: cur.PlanoFinanceiro,
        rate: cur.rate,
        ValorDaBaixaRateado: Number(cur.ValorDaBaixaRateado.toFixed(2)),
        Datadabaixa: cur.Datadabaixa,
        AcrescimoRateado: Number(cur.AcrescimoRateado.toFixed(2)),
        DescontoRateado: Number(cur.DescontoRateado.toFixed(2)),
        ValorLiquido: Number(cur.ValorLiquido.toFixed(2)),
        numConta: cur.numConta,
      });

      if (batch.length >= batchSize) {
        await bulkInsert(conn, "stg_finance_unified", columns, batch);
        batch = [];
      }
    }
  }

  if (batch.length) await bulkInsert(conn, "stg_finance_unified", columns, batch);
}
