import "dotenv/config";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { pool } from "./db/pool.js";
import { ingestDatacompet } from "./jobs/ingestDatacompet.js";
import { ingestDatapagto } from "./jobs/ingestDatapagto.js";
import { ingestExtrato } from "./jobs/ingestExtrato.js";
import { loadMergeSql } from "./sql/merge.js";

const log = pino({ level: process.env.LOG_LEVEL ?? "info" });

function mustEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Env ${name} é obrigatório`);
  return v;
}

async function main() {
  const runId = randomUUID();
  const batchSize = Number(process.env.BATCH_SIZE ?? 5000);

  const fileDatacompet = mustEnv("FILE_DATACOMPET");
  const fileDatapagto = mustEnv("FILE_DATAPAGTO");
  const fileExtrato = mustEnv("FILE_EXTRATO");

  const mergeSql = loadMergeSql();

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    // staging única — limpa por run (e depois limpa de novo no final)
    await conn.query(`DELETE FROM stg_finance_unified WHERE run_id = ?`, [runId]);

    log.info({ runId }, "Ingest BASE (SI_DATACOMPETPARCELAS)...");
    await ingestDatacompet(conn, runId, fileDatacompet, batchSize);

    log.info({ runId }, "Ingest PAY (SI_DATAPAGTO)...");
    await ingestDatapagto(conn, runId, fileDatapagto, batchSize);

    log.info({ runId }, "Ingest EXT (SI_EXTRATO_CLIENTE_HISTORICO)...");
    await ingestExtrato(conn, runId, fileExtrato, batchSize);

    log.info({ runId }, "Merge -> finance_flat (upsert only if changed)...");
    await conn.query(mergeSql, [runId]);

    // não deixar staging crescer
    await conn.query(`DELETE FROM stg_finance_unified WHERE run_id = ?`, [runId]);

    await conn.commit();
    log.info({ runId }, "DONE");
  } catch (err: any) {
    await conn.rollback();
    log.error({ runId, err: err?.message }, "FAILED");
    process.exitCode = 1;
  } finally {
    conn.release();
    await pool.end();
  }
}

main();
