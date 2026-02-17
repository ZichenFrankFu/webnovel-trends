import React, { useEffect, useState } from "react";
import { apiGet } from "../api/client";

export default function ReportsPage() {
  const [items, setItems] = useState<{ path: string; size: number }[]>([]);
  const [active, setActive] = useState<string | null>(null);
  const [content, setContent] = useState<string>("");

  useEffect(() => {
    apiGet<{ items: { path: string; size: number }[] }>("/api/reports").then((res) => setItems(res.items));
  }, []);

  async function open(path: string) {
    setActive(path);
    const res = await apiGet<{ content: string }>(`/api/reports/read?path=${encodeURIComponent(path)}`);
    setContent(res.content);
  }

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>Reports</h2>
      <div style={{ display: "flex", gap: 16 }}>
        <div style={{ width: 420, borderRight: "1px solid #eee", paddingRight: 12, height: 600, overflow: "auto" }}>
          {items
            .filter((x) => x.path.endsWith(".md") || x.path.endsWith(".html") || x.path.endsWith(".txt"))
            .map((x) => (
              <div
                key={x.path}
                onClick={() => open(x.path)}
                style={{ padding: "6px 8px", cursor: "pointer", background: active === x.path ? "#f3f3f3" : "transparent" }}
              >
                {x.path}
              </div>
            ))}
        </div>
        <div style={{ flex: 1 }}>
          <h4>{active ?? "选择一个报告"}</h4>
          <pre style={{ whiteSpace: "pre-wrap", background: "#fafafa", padding: 12, borderRadius: 8, minHeight: 600 }}>{content}</pre>
        </div>
      </div>
    </div>
  );
}
