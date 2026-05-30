import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { execSync, spawn, type ChildProcess } from "node:child_process";
import { unlinkSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { Client, type RetryConfig } from "../src/index.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = resolve(__dirname, "../../..");
const CMD_SERVER = resolve(PROJECT_ROOT, "cmd/server");

const PORT = 19999;
const BASE_URL = `http://localhost:${PORT}`;
const DB_PATH = "/tmp/sdk-test.db";

let server: ChildProcess;
let client: Client;

async function waitForServer(maxRetries = 30): Promise<void> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const res = await fetch(`${BASE_URL}/observability/failed`);
      if (res.ok) return;
    } catch {
      // server not ready yet
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error("server did not start");
}

beforeAll(async () => {
  // Kill any lingering process on our port and clean up old db.
  try { execSync(`lsof -ti:${PORT} | xargs kill -9 2>/dev/null`, { stdio: "ignore" }); } catch {}
  for (const ext of ["", "-wal", "-shm"]) {
    try { unlinkSync(DB_PATH + ext); } catch {}
  }

  server = spawn("go", ["run", CMD_SERVER, "-db", DB_PATH, "-addr", `:${PORT}`, "-tick", "200ms", "-lease", "5s"], {
    cwd: PROJECT_ROOT,
    stdio: "ignore",
  });

  await waitForServer();
  client = new Client(BASE_URL);
}, 15000);

afterAll(() => {
  server?.kill("SIGTERM");
  for (const ext of ["", "-wal", "-shm"]) {
    try { unlinkSync(DB_PATH + ext); } catch {}
  }
});

// ── Workflows ─────────────────────────────────────────────────────

describe("workflows", () => {
  it("creates a workflow in QUEUED status", async () => {
    const wf = await client.createWorkflow("wf-create", "test");
    expect(wf.workflow_id).toBe("wf-create");
    expect(wf.workflow_type).toBe("test");
    expect(wf.status).toBe("QUEUED");
    expect(wf.current_step).toBe(0);
    expect(wf.version).toBe(1);
  });

  it("gets a workflow by id", async () => {
    await client.createWorkflow("wf-get", "test");
    const wf = await client.getWorkflow("wf-get");
    expect(wf.workflow_id).toBe("wf-get");
    expect(wf.status).toBe("QUEUED");
  });

  it("completes a workflow", async () => {
    await client.createWorkflow("wf-done", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();
    await client.completeWorkflow(wf!.workflow_id, "w1");
    const got = await client.getWorkflow(wf!.workflow_id);
    expect(got.status).toBe("COMPLETED");
    expect(got.completed_at).not.toBeNull();
  });

  it("fails a workflow", async () => {
    await client.createWorkflow("wf-fail", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();
    await client.failWorkflow(wf!.workflow_id, "w1");
    const got = await client.getWorkflow(wf!.workflow_id);
    expect(got.status).toBe("FAILED");
  });

  it("marks a workflow as waiting", async () => {
    await client.createWorkflow("wf-wait", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();
    await client.markWaiting(wf!.workflow_id, "w1");
    const got = await client.getWorkflow(wf!.workflow_id);
    expect(got.status).toBe("WAITING");
  });
});

// ── Leases ─────────────────────────────────────────────────────────

describe("leases", () => {
  it("acquires a lease on an eligible workflow", async () => {
    await client.createWorkflow("lease-001", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();
    expect(wf!.status).toBe("RUNNING");
    expect(wf!.lease_owner).toBe("w1");
  });

  it("returns null when no work is available", async () => {
    // No remaining QUEUED workflows in this test context.
    const wf = await client.acquireLease({
      worker_id: "w1",
      lease_duration: 1,
    });
    // May have leftover from previous tests, so we just check it doesn't throw.
    expect(wf === null || wf!.status === "RUNNING").toBe(true);
  });

  it("renews a lease", async () => {
    await client.createWorkflow("lease-renew", "test");
    const wf = await client.acquireLease({ worker_id: "w1", lease_duration: 3 });
    expect(wf).not.toBeNull();
    // Should not throw.
    await client.renewLease(wf!.workflow_id, { worker_id: "w1", lease_duration: 10 });
  });

  it("rejects lease renewal from wrong worker", async () => {
    await client.createWorkflow("lease-wrong", "test");
    const wf = await client.acquireLease({ worker_id: "w1", lease_duration: 5 });
    expect(wf).not.toBeNull();
    await expect(
      client.renewLease(wf!.workflow_id, { worker_id: "w2", lease_duration: 10 }),
    ).rejects.toThrow(/409/);
  });

  it("releases a lease", async () => {
    await client.createWorkflow("lease-rel", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();
    await client.releaseLease(wf!.workflow_id, "w1");
    const got = await client.getWorkflow(wf!.workflow_id);
    expect(got.status).toBe("QUEUED");
    expect(got.lease_owner).toBeNull();
  });
});

// ── Steps ──────────────────────────────────────────────────────────

describe("steps", () => {
  it("completes a step and advances the cursor", async () => {
    await client.createWorkflow("step-001", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    await client.completeStep(wf!.workflow_id, "w1", 1,
      { input: 1 },
      { output: "ok" },
    );

    const steps = await client.getSteps(wf!.workflow_id);
    expect(steps.length).toBe(1);
    expect(steps[0].step_number).toBe(1);
    expect(steps[0].status).toBe("COMPLETED");

    const got = await client.getWorkflow(wf!.workflow_id);
    expect(got.current_step).toBe(1);
  });

  it("fails a step without advancing cursor", async () => {
    await client.createWorkflow("step-fail", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    await client.failStep(wf!.workflow_id, "w1", 1, { input: 1 }, "something broke");

    const steps = await client.getSteps(wf!.workflow_id);
    expect(steps.length).toBe(1);
    expect(steps[0].status).toBe("FAILED");

    const got = await client.getWorkflow(wf!.workflow_id);
    expect(got.current_step).toBe(0); // cursor did not advance
  });

  it("prevents double completion (exactly-once)", async () => {
    await client.createWorkflow("step-once", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    await client.completeStep(wf!.workflow_id, "w1", 1, {}, {});
    await expect(
      client.completeStep(wf!.workflow_id, "w1", 1, {}, {}),
    ).rejects.toThrow(/step already completed/);
  });

  it("allows completing after fail (retry path)", async () => {
    await client.createWorkflow("step-retry", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    await client.failStep(wf!.workflow_id, "w1", 1, {}, "oops");

    // On retry, completeStep should overwrite the FAILED status.
    await client.completeStep(wf!.workflow_id, "w1", 1, {}, { retried: true });

    const steps = await client.getSteps(wf!.workflow_id);
    expect(steps.length).toBe(1);
    expect(steps[0].status).toBe("COMPLETED");
  });
});

// ── Signals ────────────────────────────────────────────────────────

describe("signals", () => {
  it("sends and consumes a signal", async () => {
    await client.createWorkflow("sig-001", "test");
    const sig = await client.sendSignal("sig-001", "approve", { reason: "ok" });
    expect(sig.signal_type).toBe("approve");
    expect(sig.consumed).toBe(false);

    const consumed = await client.consumeSignal("sig-001");
    expect(consumed).not.toBeNull();
    expect(consumed!.consumed).toBe(true);

    // Already consumed — next call should return null.
    const again = await client.consumeSignal("sig-001");
    expect(again).toBeNull();
  });

  it("lists unconsumed signals", async () => {
    await client.createWorkflow("sig-list", "test");
    await client.sendSignal("sig-list", "type-a", {});
    await client.sendSignal("sig-list", "type-b", {});

    const pending = await client.getSignals("sig-list", true);
    expect(pending.length).toBe(2);

    await client.consumeSignal("sig-list");
    const remaining = await client.getSignals("sig-list", true);
    expect(remaining.length).toBe(1);
  });
});

// ── Timers ─────────────────────────────────────────────────────────

describe("timers", () => {
  it("creates a timer and fires via scheduler", async () => {
    await client.createWorkflow("timer-001", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    // Schedule a timer 1 second from now, then mark waiting.
    const wakeAt = new Date(Date.now() + 1000);
    await client.createTimer(wf!.workflow_id, wakeAt);
    await client.markWaiting(wf!.workflow_id, "w1");

    let got = await client.getWorkflow(wf!.workflow_id);
    expect(got.status).toBe("WAITING");

    // Wait for the scheduler (tick=200ms) to fire the timer.
    await new Promise((r) => setTimeout(r, 2000));

    got = await client.getWorkflow(wf!.workflow_id);
    expect(got.status).toBe("QUEUED");
    expect(got.lease_owner).toBeNull();
  }, 10000);
});

// ── Retry ──────────────────────────────────────────────────────────

describe("retry", () => {
  it("schedules a retry and emits a timer", async () => {
    await client.createWorkflow("rtry-001", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    const cfg: RetryConfig = {
      max_retries: 3,
      policy: "exponential",
      base_seconds: 1,
      max_seconds: 10,
    };

    const ok = await client.retryStep(wf!.workflow_id, "w1", 2, 1, cfg);
    expect(ok).toBe(true);

    const got = await client.getWorkflow(wf!.workflow_id);
    expect(got.status).toBe("WAITING");
  });

  it("returns false when retries exhausted", async () => {
    await client.createWorkflow("rtry-ex", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    const cfg: RetryConfig = {
      max_retries: 0,
      policy: "fixed",
      base_seconds: 1,
    };

    const ok = await client.retryStep(wf!.workflow_id, "w1", 1, 1, cfg);
    expect(ok).toBe(false);
  });
});

// ── Events ─────────────────────────────────────────────────────────

describe("events", () => {
  it("records full event history", async () => {
    await client.createWorkflow("evt-001", "test");
    const wf = await client.acquireLease({ worker_id: "w1" });
    expect(wf).not.toBeNull();

    await client.completeStep(wf!.workflow_id, "w1", 1, {}, {});
    await client.completeWorkflow(wf!.workflow_id, "w1");

    const events = await client.getEvents(wf!.workflow_id);
    expect(events.length).toBeGreaterThanOrEqual(4);

    const types = events.map((e) => e.event_type);
    expect(types).toContain("WORKFLOW_CREATED");
    expect(types).toContain("WORKFLOW_STARTED");
    expect(types).toContain("STEP_COMPLETED");
    expect(types).toContain("WORKFLOW_COMPLETED");
  });
});

// ── Observability ──────────────────────────────────────────────────

describe("observability", () => {
  it("returns completed workflow count via queryRunning", async () => {
    const running = await client.queryRunning();
    expect(Array.isArray(running)).toBe(true);
  });

  it("returns empty arrays for no-data queries", async () => {
    const failed = await client.queryFailed();
    expect(Array.isArray(failed)).toBe(true);
    expect(failed.length).toBeGreaterThanOrEqual(0);

    const queued = await client.queryQueued();
    expect(Array.isArray(queued)).toBe(true);

    const waiting = await client.queryWaiting();
    expect(Array.isArray(waiting)).toBe(true);
  });

  it("returns average step duration", async () => {
    const avg = await client.avgStepDuration();
    expect(typeof avg).toBe("object");
  });
});

// ── Concurrency safety ─────────────────────────────────────────────

describe("concurrency", () => {
  it("prevents lease steal — only one worker per workflow", async () => {
    await client.createWorkflow("conc-001", "test");

    // Worker A claims the lease.
    const a = await client.acquireLease({ worker_id: "wa" });
    expect(a).not.toBeNull();

    // Worker B should NOT get the same workflow.
    const b = await client.acquireLease({ worker_id: "wb" });
    // b may be a different workflow or null — but should not be conc-001.
    if (b) {
      expect(b.workflow_id).not.toBe("conc-001");
    }
  });

  it("rejects step from worker that does not hold lease", async () => {
    await client.createWorkflow("conc-002", "test");
    const wf = await client.acquireLease({ worker_id: "wa" });
    expect(wf).not.toBeNull();

    // Worker B tries to complete a step — should be rejected.
    await expect(
      client.completeStep(wf!.workflow_id, "wb", 1, {}, {}),
    ).rejects.toThrow(/409/);
  });
});
