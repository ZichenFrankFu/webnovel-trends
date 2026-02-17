import React, { useState } from "react";
import ConfigPage from "./pages/ConfigPage";
import RunnerPage from "./pages/RunnerPage";
import ReportsPage from "./pages/ReportsPage";
import DatabasePage from "./pages/DatabasePage";

type Tab = "config" | "runner" | "reports" | "db";

export default function App() {
  const [tab, setTab] = useState<Tab>("config");
  const [lastRunId, setLastRunId] = useState<string | null>(null);

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "system-ui" }}>
      <div style={{ width: 220, borderRight: "1px solid #ddd", padding: 12 }}>
        <div style={{ fontWeight: 700, marginBottom: 12 }}>WebNovel Trends UI</div>
        <NavButton label="Config" active={tab === "config"} onClick={() => setTab("config")} />
        <NavButton label="Runner" active={tab === "runner"} onClick={() => setTab("runner")} />
        <NavButton label="Reports" active={tab === "reports"} onClick={() => setTab("reports")} />
        <NavButton label="Database" active={tab === "db"} onClick={() => setTab("db")} />
        <div style={{ marginTop: 16, fontSize: 12, color: "#555" }}>
          当前 config_run: {lastRunId ?? "(none)"}
        </div>
      </div>

      <div style={{ flex: 1, padding: 16, overflow: "auto" }}>
        {tab === "config" && <ConfigPage onSaved={(id) => setLastRunId(id)} />}
        {tab === "runner" && <RunnerPage lastRunId={lastRunId} />}
        {tab === "reports" && <ReportsPage />}
        {tab === "db" && <DatabasePage />}
      </div>
    </div>
  );
}

function NavButton(props: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={props.onClick}
      style={{
        width: "100%",
        padding: "10px 12px",
        marginBottom: 8,
        borderRadius: 8,
        border: "1px solid #ddd",
        background: props.active ? "#eee" : "white",
        cursor: "pointer",
        textAlign: "left",
      }}
    >
      {props.label}
    </button>
  );
}
