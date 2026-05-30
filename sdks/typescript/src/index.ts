export type WorkflowStatus = "QUEUED" | "RUNNING" | "WAITING" | "COMPLETED" | "FAILED";

export type EventType =
  | "WORKFLOW_CREATED"
  | "WORKFLOW_STARTED"
  | "STEP_STARTED"
  | "STEP_COMPLETED"
  | "STEP_FAILED"
  | "SIGNAL_RECEIVED"
  | "TIMER_FIRED"
  | "WORKFLOW_COMPLETED"
  | "WORKFLOW_FAILED";

export interface Workflow {
  workflow_id: string;
  workflow_type: string;
  status: WorkflowStatus;
  current_step: number;
  lease_owner: string | null;
  lease_expires_at: number | null;
  created_at: number;
  updated_at: number;
  completed_at: number | null;
  version: number;
}

export interface WorkflowEvent {
  event_id: number;
  workflow_id: string;
  event_type: EventType;
  payload_json: string | null;
  sequence_number: number;
  created_at: number;
}

export interface WorkflowStep {
  workflow_id: string;
  step_number: number;
  status: string;
  input_json: string | null;
  output_json: string | null;
  started_at: number | null;
  completed_at: number | null;
}

export interface Signal {
  signal_id: number;
  workflow_id: string;
  signal_type: string;
  payload_json: string | null;
  consumed: boolean;
  created_at: number;
}

export interface Timer {
  timer_id: number;
  workflow_id: string;
  wake_at: number;
  fired: boolean;
}

export type BackoffPolicy = "fixed" | "linear" | "exponential";

export interface RetryConfig {
  max_retries: number;
  policy: BackoffPolicy;
  base_seconds: number;
  max_seconds?: number;
}

export interface LeaseConfig {
  worker_id: string;
  lease_duration?: number; // seconds
}

// ── Client ─────────────────────────────────────────────────────

export interface ClientOptions {
  apiKey?: string;
  timeout?: number;
  maxRetries?: number;
  retryDelay?: number;
}

export class Client {
  private baseUrl: string;
  private apiKey: string | undefined;
  private timeout: number;
  private maxRetries: number;
  private retryDelay: number;

  constructor(baseUrl = "http://localhost:8080", opts?: ClientOptions) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.apiKey = opts?.apiKey;
    this.timeout = opts?.timeout ?? 30_000;
    this.maxRetries = opts?.maxRetries ?? 0;
    this.retryDelay = opts?.retryDelay ?? 500;
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.apiKey) {
      headers["Authorization"] = `Bearer ${this.apiKey}`;
    }

    let lastError: Error | undefined;
    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);
        const res = await fetch(`${this.baseUrl}${path}`, {
          method,
          headers,
          body: body ? JSON.stringify(body) : undefined,
          signal: controller.signal,
        });
        clearTimeout(timer);
        const data = await res.json();
        if (!res.ok) {
          throw new Error(
            `HTTP ${res.status}: ${data.error ?? JSON.stringify(data)}`,
          );
        }
        return data as T;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        if (attempt < this.maxRetries) {
          await new Promise((r) => setTimeout(r, this.retryDelay));
        }
      }
    }
    throw lastError;
  }

  // ── Workflows ──────────────────────────────────────────────

  async createWorkflow(workflowId: string, workflowType: string): Promise<Workflow> {
    return this.request("POST", "/workflows", {
      workflow_id: workflowId,
      workflow_type: workflowType,
    });
  }

  async getWorkflow(workflowId: string): Promise<Workflow> {
    return this.request("GET", `/workflows/${workflowId}`);
  }

  async completeWorkflow(workflowId: string, workerId: string): Promise<void> {
    await this.request("POST", `/workflows/${workflowId}/complete`, {
      worker_id: workerId,
    });
  }

  async failWorkflow(workflowId: string, workerId: string): Promise<void> {
    await this.request("POST", `/workflows/${workflowId}/fail`, {
      worker_id: workerId,
    });
  }

  async markWaiting(workflowId: string, workerId: string): Promise<void> {
    await this.request("POST", `/workflows/${workflowId}/waiting`, {
      worker_id: workerId,
    });
  }

  // ── Leases ─────────────────────────────────────────────────

  async acquireLease(cfg: LeaseConfig): Promise<Workflow | null> {
    const data = await this.request<Workflow | { status: string }>(
      "POST",
      "/leases/acquire",
      {
        worker_id: cfg.worker_id,
        lease_duration: cfg.lease_duration,
      },
    );
    if ("status" in data && data.status === "no_work") return null;
    return data as Workflow;
  }

  async renewLease(
    workflowId: string,
    cfg: LeaseConfig,
  ): Promise<void> {
    await this.request("POST", `/leases/${workflowId}/renew`, {
      worker_id: cfg.worker_id,
      lease_duration: cfg.lease_duration,
    });
  }

  async releaseLease(workflowId: string, workerId: string): Promise<void> {
    await this.request("POST", `/leases/${workflowId}/release`, {
      worker_id: workerId,
    });
  }

  // ── Steps ─────────────────────────────────────────────────

  async startStep(
    workflowId: string,
    workerId: string,
    stepNumber: number,
    input?: unknown,
  ): Promise<void> {
    await this.request("POST", "/steps/start", {
      workflow_id: workflowId,
      worker_id: workerId,
      step_number: stepNumber,
      input,
    });
  }

  async completeStep(
    workflowId: string,
    workerId: string,
    stepNumber: number,
    input?: unknown,
    output?: unknown,
  ): Promise<void> {
    await this.request("POST", "/steps/complete", {
      workflow_id: workflowId,
      worker_id: workerId,
      step_number: stepNumber,
      input,
      output,
    });
  }

  async failStep(
    workflowId: string,
    workerId: string,
    stepNumber: number,
    input?: unknown,
    error?: string,
  ): Promise<void> {
    await this.request("POST", "/steps/fail", {
      workflow_id: workflowId,
      worker_id: workerId,
      step_number: stepNumber,
      input,
      error: error ?? "unknown error",
    });
  }

  async getSteps(workflowId: string): Promise<WorkflowStep[]> {
    return this.request("GET", `/steps/${workflowId}`);
  }

  // ── Signals ───────────────────────────────────────────────

  async sendSignal(
    workflowId: string,
    signalType: string,
    payload?: unknown,
  ): Promise<Signal> {
    return this.request("POST", "/signals", {
      workflow_id: workflowId,
      signal_type: signalType,
      payload,
    });
  }

  async consumeSignal(workflowId: string): Promise<Signal | null> {
    const data = await this.request<Signal | { status: string }>(
      "POST",
      `/signals/${workflowId}/consume`,
    );
    if ("status" in data) return null;
    return data as Signal;
  }

  async getSignals(
    workflowId: string,
    unconsumedOnly = false,
  ): Promise<Signal[]> {
    const qs = unconsumedOnly ? "?unconsumed=true" : "";
    return this.request("GET", `/signals/${workflowId}${qs}`);
  }

  // ── Timers ────────────────────────────────────────────────

  async createTimer(
    workflowId: string,
    wakeAt: Date,
  ): Promise<Timer> {
    return this.request("POST", "/timers", {
      workflow_id: workflowId,
      wake_at: Math.floor(wakeAt.getTime() / 1000),
    });
  }

  // ── Retry ─────────────────────────────────────────────────

  async retryStep(
    workflowId: string,
    workerId: string,
    stepNumber: number,
    retryCount: number,
    cfg: RetryConfig,
  ): Promise<boolean> {
    const data = await this.request<{ retry_scheduled: boolean }>(
      "POST",
      "/retry",
      {
        workflow_id: workflowId,
        worker_id: workerId,
        step_number: stepNumber,
        retry_count: retryCount,
        max_retries: cfg.max_retries,
        policy: cfg.policy,
        base_seconds: cfg.base_seconds,
        max_seconds: cfg.max_seconds ?? 0,
      },
    );
    return data.retry_scheduled;
  }

  // ── Events ────────────────────────────────────────────────

  async getEvents(workflowId: string): Promise<WorkflowEvent[]> {
    return this.request("GET", `/events/${workflowId}`);
  }

  // ── Observability ─────────────────────────────────────────

  async queryFailed(): Promise<Workflow[]> {
    const data = await this.request<{ items: Workflow[] }>(
      "GET",
      "/observability/failed",
    );
    return data.items;
  }

  async queryRunning(): Promise<Workflow[]> {
    const data = await this.request<{ items: Workflow[] }>(
      "GET",
      "/observability/running",
    );
    return data.items;
  }

  async queryQueued(): Promise<Workflow[]> {
    const data = await this.request<{ items: Workflow[] }>(
      "GET",
      "/observability/queued",
    );
    return data.items;
  }

  async queryWaiting(): Promise<Workflow[]> {
    const data = await this.request<{ items: Workflow[] }>(
      "GET",
      "/observability/waiting",
    );
    return data.items;
  }

  async avgStepDuration(): Promise<Record<number, number>> {
    return this.request("GET", "/observability/avg-step-duration");
  }
}
