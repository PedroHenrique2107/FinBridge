import type { PoolConnection } from "mysql2/promise";

export type Row = Record<string, any>;

export async function bulkInsert(
  conn: PoolConnection,
  table: string,
  columns: string[],
  rows: Row[]
): Promise<void> {
  if (!rows.length) return;

  const values: any[] = [];
  const placeholders = rows
    .map((r) => {
      const rowPlaceholders = columns.map(() => "?").join(",");
      for (const c of columns) values.push(r[c] ?? null);
      return `(${rowPlaceholders})`;
    })
    .join(",");

  const sql = `INSERT INTO ${table} (${columns.join(",")}) VALUES ${placeholders}`;
  await conn.query(sql, values);
}
