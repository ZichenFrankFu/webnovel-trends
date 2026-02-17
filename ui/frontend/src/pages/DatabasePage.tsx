import React, { useEffect, useState } from "react";
import { apiGet } from "../api/client";

export default function DatabasePage() {
  const [tables, setTables] = useState<string[]>([]);
  const [activeTable, setActiveTable] = useState<string | null>(null);
  const [rows, setRows] = useState<any[]>([]);

  const [rankLists, setRankLists] = useState<any[]>([]);
  const [snapshots, setSnapshots] = useState<any[]>([]);
  const [entries, setEntries] = useState<any[]>([]);

  const [mismatch, setMismatch] = useState<any[]>([]);

  useEffect(() => {
    apiGet<{ tables: string[] }>("/api/db/tables").then((res) => setTables(res.tables));
    apiGet<{ rows: any[] }>("/api/db/rank_lists").then((res) => setRankLists(res.rows));
  }, []);

  async function openTable(name: string) {
    setActiveTable(name);
    const res = await apiGet<{ rows: any[] }>(`/api/db/table/${encodeURIComponent(name)}?limit=50&offset=0`);
    setRows(res.rows);
  }

  async function openSnapshots(rank_list_id: number) {
    const res = await apiGet<{ rows: any[] }>(`/api/db/snapshots?rank_list_id=${rank_list_id}`);
    setSnapshots(res.rows);
    setEntries([]);
  }

  async function openEntries(snapshot_id: number) {
    const res = await apiGet<{ rows: any[] }>(`/api/db/entries?snapshot_id=${snapshot_id}&limit=200`);
    setEntries(res.rows);
  }

  async function runMismatch() {
    const res = await apiGet<{ rows: any[] }>(`/api/db/diagnostics/item_count_mismatch?limit=200`);
    setMismatch(res.rows);
  }

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>Database</h2>

      <h3>Diagnostics</h3>
      <button onClick={runMismatch} style={{ padding: "8px 10px", borderRadius: 8, border: "1px solid #ddd", cursor: "pointer" }}>
        Find item_count mismatch
      </button>
      {mismatch.length > 0 && (
        <pre style={{ whiteSpace: "pre-wrap", background: "#fafafa", padding: 10, borderRadius: 8, marginTop: 8 }}>
          {JSON.stringify(mismatch.slice(0, 20), null, 2)}
        </pre>
      )}

      <h3>Rank Drilldown</h3>
      <div style={{ display: "flex", gap: 16 }}>
        <div style={{ width: 360 }}>
          <div style={{ fontWeight: 700 }}>rank_lists</div>
          <div style={{ height: 240, overflow: "auto", border: "1px solid #eee", borderRadius: 8 }}>
            {rankLists.map((r) => (
              <div key={r.rank_list_id} style={{ padding: 8, cursor: "pointer" }} onClick={() => openSnapshots(r.rank_list_id)}>
                #{r.rank_list_id} {r.platform} {r.rank_family} {r.rank_sub_cat}
              </div>
            ))}
          </div>
        </div>

        <div style={{ width: 420 }}>
          <div style={{ fontWeight: 700 }}>rank_snapshots</div>
          <div style={{ height: 240, overflow: "auto", border: "1px solid #eee", borderRadius: 8 }}>
            {snapshots.map((s) => (
              <div key={s.snapshot_id} style={{ padding: 8, cursor: "pointer" }} onClick={() => openEntries(s.snapshot_id)}>
                snapshot_id={s.snapshot_id} date={s.snapshot_date} item_count={s.item_count}
              </div>
            ))}
          </div>
        </div>

        <div style={{ flex: 1 }}>
          <div style={{ fontWeight: 700 }}>rank_entries (top 200)</div>
          <pre style={{ whiteSpace: "pre-wrap", background: "#fafafa", padding: 10, borderRadius: 8, height: 240, overflow: "auto" }}>
            {entries.length ? JSON.stringify(entries.slice(0, 30), null, 2) : "(select a snapshot)"}
          </pre>
        </div>
      </div>

      <h3>Table Browser</h3>
      <div style={{ display: "flex", gap: 16 }}>
        <div style={{ width: 240 }}>
          <div style={{ height: 260, overflow: "auto", border: "1px solid #eee", borderRadius: 8 }}>
            {tables.map((t) => (
              <div key={t} style={{ padding: 8, cursor: "pointer", background: activeTable === t ? "#f3f3f3" : "transparent" }} onClick={() => openTable(t)}>
                {t}
              </div>
            ))}
          </div>
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ fontWeight: 700 }}>{activeTable ?? "select a table"}</div>
          <pre style={{ whiteSpace: "pre-wrap", background: "#fafafa", padding: 10, borderRadius: 8, minHeight: 260 }}>
            {rows.length ? JSON.stringify(rows.slice(0, 30), null, 2) : "(no rows)"}
          </pre>
        </div>
      </div>
    </div>
  );
}
