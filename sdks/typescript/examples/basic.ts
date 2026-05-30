import { Client, type RetryConfig } from "../src/index.js";

const SERVER_URL = process.env.SERVER_URL ?? "http://localhost:8080";
const client = new Client(SERVER_URL);

async function main() {
  // ── Create workflows ───────────────────────────────────────
  const wf1 = await client.createWorkflow("order-001", "order_processing");
  const wf2 = await client.createWorkflow("order-002", "order_processing");
  console.log(`[create] ${wf1.workflow_id} → ${wf1.status}`);
  console.log(`[create] ${wf2.workflow_id} → ${wf2.status}`);

  // ── Acquire lease ──────────────────────────────────────────
  let wf = await client.acquireLease({ worker_id: "worker-a" });
  if (!wf) {
    console.log("[lease]  no work available");
    return;
  }
  console.log(`[lease]  ${wf.workflow_id} claimed by worker-a → ${wf.status}`);

  // ── Execute steps with retry ───────────────────────────────
  const retryCfg: RetryConfig = {
    max_retries: 2,
    policy: "exponential",
    base_seconds: 1,
    max_seconds: 10,
  };

  for (let step = 1; step <= 3; step++) {
    const input = { step };

    if (step === 2) {
      // Simulate a recoverable failure.
      console.log(`[step]   step ${step} failed — scheduling retry`);
      await client.failStep(wf.workflow_id, "worker-a", step, input, "transient error");
      const ok = await client.retryStep(wf.workflow_id, "worker-a", step, 1, retryCfg);
      if (!ok) {
        await client.failWorkflow(wf.workflow_id, "worker-a");
        console.log("[fail]   max retries exceeded");
        return;
      }
      break;
    }

    const output = { result: `ok-${step}` };
    await client.completeStep(wf.workflow_id, "worker-a", step, input, output);
    console.log(`[step]   step ${step} completed`);
  }

  // ── Send signal ────────────────────────────────────────────
  const sig = await client.sendSignal("order-002", "approval_requested", {
    approved_by: "manager",
  });
  console.log(`[signal] sent signal ${sig.signal_id} to order-002`);

  const pending = await client.getSignals("order-002", true);
  console.log(`[signal] pending signals for order-002: ${pending.length}`);

  // ── Create timer ───────────────────────────────────────────
  const wakeAt = new Date(Date.now() + 2_000);
  const timer = await client.createTimer("order-002", wakeAt);
  console.log(
    `[timer]  created timer ${timer.timer_id} wakes at ${wakeAt.toLocaleTimeString()}`,
  );

  // ── Wait for scheduler to process ──────────────────────────
  console.log("[wait]   waiting 3s for scheduler...");
  await sleep(3_000);

  // ── Re-acquire retried workflow ────────────────────────────
  wf = await client.acquireLease({ worker_id: "worker-a" });
  if (!wf) {
    console.log("[retry]  no work to claim");
    return;
  }
  console.log(`[retry]  re-acquired ${wf.workflow_id} (step ${wf.current_step})`);

  for (let step = wf.current_step + 1; step <= 3; step++) {
    const input = { step };
    const output = { result: `ok-${step}` };
    await client.completeStep(wf.workflow_id, "worker-a", step, input, output);
    console.log(`[step]   step ${step} completed`);
  }

  await client.completeWorkflow(wf.workflow_id, "worker-a");
  console.log(`[done]   ${wf.workflow_id} → COMPLETED`);

  // ── Observability ──────────────────────────────────────────
  console.log("\n── Observability ──");
  console.log(`  failed:   ${(await client.queryFailed()).length}`);
  console.log(`  running:  ${(await client.queryRunning()).length}`);
  console.log(`  queued:   ${(await client.queryQueued()).length}`);
  console.log(`  waiting:  ${(await client.queryWaiting()).length}`);
  console.log(`  avg step: ${JSON.stringify(await client.avgStepDuration())}`);

  // ── Event history ──────────────────────────────────────────
  const events = await client.getEvents("order-001");
  console.log("\n  events for order-001:");
  for (const ev of events) {
    const payload = ev.payload_json ?? "<nil>";
    console.log(`    [${ev.sequence_number}] ${ev.event_type} ${payload}`);
  }
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
