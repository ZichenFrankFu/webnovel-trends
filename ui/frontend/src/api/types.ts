export type ConfigSchema = {
  defaults: any;
  rank_keys: { qidian: string[]; fanqie: string[] };
  notes: Record<string, string>;
};

export type Task = {
  task_id: string;
  task_type: string;
  status: string;
  created_at: number;
  started_at?: number | null;
  ended_at?: number | null;
  config_run_id?: string | null;
  command?: string[] | null;
  log_path?: string | null;
  exit_code?: number | null;
};
