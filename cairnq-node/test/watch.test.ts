import { describe, expect, it, vi } from "vitest";

import {
  CairnQ,
  type PgExecutor,
  type PgSession,
  PostgresStore,
  type Row,
  type WatchSignal,
} from "../src/index.js";

// watch() is specified as notify-ACCELERATED POLLING: a signal means "re-read",
// the timer guarantees delivery, and the push channel only makes it prompt. These
// pin both halves — the promptness AND the fallback — because an API that
// silently stops signalling behind a pooler is worse than one that never claimed
// to push at all.

function listeningExecutor() {
  let notify: ((channel: string, payload: string) => void) | null = null;
  let subscriptions = 0;
  const session: PgSession = {
    async query(text: string): Promise<Row[]> {
      return /protocol_version/.test(text) && /select/i.test(text) ? [{ value: "1" }] : [];
    },
    async exec(): Promise<void> {},
  };
  const executor: PgExecutor = {
    query: session.query,
    exec: session.exec,
    async tx<T>(fn: (s: PgSession) => Promise<T>): Promise<T> {
      return fn(session);
    },
    async listen(_channels, onNotify) {
      subscriptions++;
      notify = onNotify as typeof notify;
      return () => {
        notify = null;
      };
    },
    async close() {},
  };
  return {
    executor,
    get subscriptions() {
      return subscriptions;
    },
    async emit(channel: string, payload: string) {
      // The subscription is established in the background on the first watch().
      for (let i = 0; i < 50 && !notify; i++) await new Promise((r) => setTimeout(r, 2));
      notify?.(channel, payload);
    },
  };
}

describe("watch", () => {
  it("delivers a queued signal naming its queue, and a done signal naming its task", async () => {
    const fake = listeningExecutor();
    const store = new PostgresStore(fake.executor);
    await store.connect();

    const seen: WatchSignal[] = [];
    // A poll interval far beyond the test, so anything observed came from push.
    const stop = store.watch({ pollMs: 60_000 }, (s) => seen.push(s));
    try {
      await fake.emit("cairnq_queued", "render");
      await fake.emit("cairnq_done", "task-7");
      expect(seen).toEqual([
        { reason: "queued", queue: "render" },
        { reason: "done", taskId: "task-7" },
      ]);
    } finally {
      stop();
    }
  });

  it("drops queued signals for queues this watch did not ask about", async () => {
    const fake = listeningExecutor();
    const store = new PostgresStore(fake.executor);
    await store.connect();

    const seen: WatchSignal[] = [];
    const stop = store.watch({ queues: ["render"], pollMs: 60_000 }, (s) => seen.push(s));
    try {
      await fake.emit("cairnq_queued", "ingest");
      await fake.emit("cairnq_queued", "render");
      // A done notification names only the task — which queue it was on is not in
      // the payload, so it is never filtered out.
      await fake.emit("cairnq_done", "task-7");
      expect(seen).toEqual([
        { reason: "queued", queue: "render" },
        { reason: "done", taskId: "task-7" },
      ]);
    } finally {
      stop();
    }
  });

  // close() does not await a connect that is still in flight — it only drops the
  // handle to it. That connect resumes afterwards, and if it publishes what it
  // built, a store nobody will close again ends up owning a LISTEN connection
  // whose socket keeps the process alive by itself.
  it("does not install a listener for a store closed while it was connecting", async () => {
    const fake = listeningExecutor();
    let releaseMigrations!: () => void;
    const gate = new Promise<void>((r) => (releaseMigrations = r));
    const slow: PgExecutor = {
      ...fake.executor,
      // Stands in for the migration round-trips: connect is mid-flight here.
      async exec(): Promise<void> {
        await gate;
      },
    };
    const store = new PostgresStore(slow);

    const connecting = store.connect();
    await store.close();
    releaseMigrations();
    await expect(connecting).rejects.toThrow(/closed while connecting/);

    // Nothing subscribed, and nothing can start one later either.
    expect(fake.subscriptions).toBe(0);
    store.watch({ pollMs: 60_000 }, () => {})();
    await new Promise((r) => setTimeout(r, 20));
    expect(fake.subscriptions).toBe(0);
  });

  // claimWake's buffer, which watch() shares a listener with. A notification that
  // lands between two polls has to survive until the next claimWake asks — but
  // only for a queue somebody actually waits on, or the buffer is a leak that
  // grows with every distinct queue name the database ever sees.
  it("buffers a wake for a queue that is waited on, and only for those", async () => {
    const fake = listeningExecutor();
    const store = new PostgresStore(fake.executor);
    await store.connect();
    // Establishes both the subscription and what this process waits on.
    await store.claimWake(["render"], 1);

    await fake.emit("cairnq_queued", "ingest");
    await fake.emit("cairnq_queued", "user:1234");
    // Nothing waits on those, so nothing is remembered about them.
    expect([...(store as unknown as { pendingQueues: Set<string> }).pendingQueues]).toEqual([]);

    await fake.emit("cairnq_queued", "render");
    const startedAt = Date.now();
    // Buffered while nobody was waiting, so this returns on the notification
    // rather than waiting out its timeout.
    await store.claimWake(["render"], 60_000);
    expect(Date.now() - startedAt).toBeLessThan(1_000);

    await store.close();
  });

  it("keeps signalling on the timer where there is no push channel at all", async () => {
    vi.useFakeTimers();
    try {
      // SQLite has no channel; the consumer must still be told to re-read.
      const tasks = CairnQ.sqlite(":memory:");
      const seen: WatchSignal[] = [];
      const stop = tasks.watch({ pollMs: 50 }, (s) => seen.push(s));
      await vi.advanceTimersByTimeAsync(160);
      stop();
      expect(seen.length).toBeGreaterThanOrEqual(3);
      expect(seen.every((s) => s.reason === "poll")).toBe(true);
      await tasks.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("stops signalling once unsubscribed", async () => {
    vi.useFakeTimers();
    try {
      const tasks = CairnQ.sqlite(":memory:");
      const seen: WatchSignal[] = [];
      const stop = tasks.watch({ pollMs: 50 }, (s) => seen.push(s));
      await vi.advanceTimersByTimeAsync(120);
      const atStop = seen.length;
      stop();
      await vi.advanceTimersByTimeAsync(500);
      // A signal after unsubscribe would have the consumer re-reading a store it
      // has stopped caring about — possibly a closed one.
      expect(seen.length).toBe(atStop);
      await tasks.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not let one subscriber's exception cost the others their signal", async () => {
    const fake = listeningExecutor();
    const store = new PostgresStore(fake.executor);
    await store.connect();

    const seen: WatchSignal[] = [];
    const stopA = store.watch({ pollMs: 60_000 }, () => {
      throw new Error("consumer bug");
    });
    const stopB = store.watch({ pollMs: 60_000 }, (s) => seen.push(s));
    try {
      await fake.emit("cairnq_queued", "render");
      expect(seen).toHaveLength(1);
    } finally {
      stopA();
      stopB();
    }
  });

  it("shares one subscription across watchers and keeps the channel warm", async () => {
    vi.useFakeTimers();
    const fake = listeningExecutor();
    const store = new PostgresStore(fake.executor);
    try {
      await store.connect();
      const stopA = store.watch({ pollMs: 50 }, () => {});
      const stopB = store.watch({ pollMs: 50 }, () => {});
      await vi.advanceTimersByTimeAsync(200);
      // One LISTEN connection serves every watcher; the timer is what would
      // re-establish it after a drop, which is the only thing keeping a
      // client-side watcher alive in a process that never claims.
      expect(fake.subscriptions).toBe(1);
      stopA();
      stopB();
    } finally {
      vi.useRealTimers();
    }
  });
});
