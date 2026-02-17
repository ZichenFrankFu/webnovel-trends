import React, { useEffect, useRef, useState } from "react";
import { apiGet } from "../api/client";

export default function LogViewer(props: { taskId: string }) {
  const [text, setText] = useState("");
  const [offset, setOffset] = useState(0);
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function tick() {
      try {
        const res = await apiGet<{ offset: number; text: string }>(`/api/tasks/${props.taskId}/logs?offset=${offset}`);
        if (!cancelled && res.text) {
          setText((t) => t + res.text);
          setOffset(res.offset);
        }
      } catch (e) {
        // ignore transient
      } finally {
        if (!cancelled) {
          timerRef.current = window.setTimeout(tick, 1000);
        }
      }
    }

    tick();
    return () => {
      cancelled = true;
      if (timerRef.current) window.clearTimeout(timerRef.current);
    };
  }, [props.taskId, offset]);

  return (
    <pre style={{ background: "#111", color: "#eee", padding: 12, borderRadius: 8, height: 360, overflow: "auto" }}>
      {text || "(no logs yet)"}
    </pre>
  );
}
