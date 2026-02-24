import fs from "node:fs";
import path from "node:path";

export function loadMergeSql(): string {
  const candidates = [
    path.resolve(process.cwd(), "sql", "002_merge.sql"),
    path.resolve(process.cwd(), "..", "sql", "002_merge.sql"),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) return fs.readFileSync(p, "utf8");
  }
  throw new Error("Não encontrei sql/002_merge.sql");
}
