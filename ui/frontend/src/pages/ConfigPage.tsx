import React, { useEffect, useMemo, useState } from "react";
import { apiGet, apiPost } from "../api/client";
import type { ConfigSchema } from "../api/types";

export default function ConfigPage(props: { onSaved: (runId: string) => void }) {
  const [schema, setSchema] = useState<ConfigSchema | null>(null);
  const [form, setForm] = useState<any>({
    platform: "fanqie",
    rank_key: "",
    pages: null,
    qidian_pages: 2,
    chapter_count: 5,
    newbook_chapter_count: 2,
    no_detail: false,
    no_chapters: false,
  });
  const rankKeys = useMemo(() => {
    if (!schema) return [];
    return form.platform === "qidian" ? schema.rank_keys.qidian : schema.rank_keys.fanqie;
  }, [schema, form.platform]);

  useEffect(() => {
    apiGet<ConfigSchema>("/api/config/schema").then(setSchema).catch((e) => alert(String(e)));
  }, []);

  async function save() {
    const res = await apiPost<{ run_id: string; path: string }>("/api/config/runs", form);
    props.onSaved(res.run_id);
    alert(`Saved config run: ${res.run_id}`);
  }

  if (!schema) return <div>Loading schema...</div>;

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>Config</h2>

      <Row label="platform">
        <select value={form.platform ?? ""} onChange={(e) => setForm({ ...form, platform: e.target.value, rank_key: "" })}>
          <option value="qidian">qidian</option>
          <option value="fanqie">fanqie</option>
        </select>
      </Row>

      <Row label="rank_key（可选：留空=平台全榜）">
        <select value={form.rank_key ?? ""} onChange={(e) => setForm({ ...form, rank_key: e.target.value })}>
          <option value="">(ALL ranks)</option>
          {rankKeys.map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </select>
      </Row>

      {form.platform === "qidian" && (
        <Row label="pages（起点单榜/全榜页数）">
          <input
            type="number"
            value={form.pages ?? ""}
            placeholder="(empty = use qidian_pages)"
            onChange={(e) => setForm({ ...form, pages: e.target.value === "" ? null : Number(e.target.value) })}
          />
        </Row>
      )}

      <Row label="qidian_pages（legacy fallback）">
        <input type="number" value={form.qidian_pages} onChange={(e) => setForm({ ...form, qidian_pages: Number(e.target.value) })} />
      </Row>

      <Row label="chapter_count">
        <input type="number" value={form.chapter_count} onChange={(e) => setForm({ ...form, chapter_count: Number(e.target.value) })} />
      </Row>

      <Row label="newbook_chapter_count">
        <input
          type="number"
          value={form.newbook_chapter_count}
          onChange={(e) => setForm({ ...form, newbook_chapter_count: Number(e.target.value) })}
        />
      </Row>

      <Row label="no_detail">
        <input type="checkbox" checked={!!form.no_detail} onChange={(e) => setForm({ ...form, no_detail: e.target.checked })} />
      </Row>

      <Row label="no_chapters">
        <input type="checkbox" checked={!!form.no_chapters} onChange={(e) => setForm({ ...form, no_chapters: e.target.checked })} />
      </Row>

      <button onClick={save} style={{ padding: "10px 12px", borderRadius: 8, border: "1px solid #ddd", cursor: "pointer" }}>
        Save Config Run
      </button>

      <div style={{ marginTop: 16, fontSize: 12, color: "#666" }}>
        <div>Notes:</div>
        <pre style={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(schema.notes, null, 2)}</pre>
      </div>
    </div>
  );
}

function Row(props: { label: string; children: React.ReactNode }) {
  return (
    <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 10 }}>
      <div style={{ width: 220, color: "#333" }}>{props.label}</div>
      <div>{props.children}</div>
    </div>
  );
}
