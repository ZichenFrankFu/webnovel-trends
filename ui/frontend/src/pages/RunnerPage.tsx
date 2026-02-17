import React, { useEffect, useState } from "react";
import { apiGet, apiPost } from "../api/client";
import type { Task } from "../api/types";
import LogViewer from "../components/LogViewer";

export default function RunnerPage(props: { lastRunId: string | null }) {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [activeTaskId, setActiveTaskId] = useState<string | null>(null);

  async function refresh() {
    const res = await apiGet<{ tasks: Task[] }>("/api/tasks");
    setTasks(res.tasks);
  }

  useEffect(() => {
    refresh();
    const t = window.setInterval(refresh, 2000);
    return () => window.clearInterval(t);
  }, []);

  async function start() {
    if (!props.lastRunId) {
      alert("先去 Config 页保存一个 config run");
      return;
    }
    const res = await apiPost<{ task_id: string }>("/api/tasks/spider?run_id=" + encodeURIComponent(props.lastRunId), null);
    setActiveTaskId(res.task_id);
    refresh();
  }

  return (
    <div>
      <h2 style={{ marginTop: 0 }}>Runner</h2>

      <button onClick={start} style={{ padding: "10px 12px", borderRadius: 8, border: "1px solid #ddd", cursor: "pointer" }}>
        Start Spider (main.py once)
      </button>

      <h3>Tasks</h3>
      <div style={{ display: "flex", gap: 16 }}>
        <div style={{ minWidth: 420 }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th align="left">task_id</th>
                <th align="left">type</th>
                <th align="left">status</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((t) => (
                <tr
                  key={t.task_id}
                  onClick={() => setActiveTaskId(t.task_id)}
                  style={{ cursor: "pointer", background: activeTaskId === t.task_id ? "#f3f3f3" : "transparent" }}
                >
                  <td style={{ borderTop: "1px solid #eee", padding: 6 }}>{t.task_id}</td>
                  <td style={{ borderTop: "1px solid #eee", padding: 6 }}>{t.task_type}</td>
                  <td style={{ borderTop: "1px solid #eee", padding: 6 }}>{t.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div style={{ flex: 1 }}>
          <h4>Logs</h4>
          {activeTaskId ? <LogViewer taskId={activeTaskId} /> : <div>点击左侧任务查看日志</div>}
        </div>
      </div>
    </div>
  );
}
