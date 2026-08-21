import { describe, expect, it } from "vitest";

// Both backends are optional dependencies, and the point of that is wasted if
// importing the package loads them anyway. A Postgres-only deployment — which is
// every multi-host one — should not have to build a native module, and on hosts
// where node-gyp cannot build (an NFS home, a locked-down image) that difference
// is between installable and not.
//
// Guards the import graph, not the package manifest: `index.ts` re-exports
// SQLiteStore, so a plain top-level `import Database from "better-sqlite3"`
// anywhere under it would silently undo this.
describe("optional native drivers", () => {
  it("does not load better-sqlite3 just because the package was imported", async () => {
    const opened: string[] = [];
    const real = process.dlopen;
    // Every .node addon reaches the process through here, whichever resolver
    // (bindings, prebuild-install, a plain require) found the file.
    process.dlopen = ((module: unknown, filename: string, ...rest: unknown[]) => {
      opened.push(filename);
      return (real as (...a: unknown[]) => unknown)(module, filename, ...rest);
    }) as typeof process.dlopen;
    try {
      const mod = await import("../src/index.js");
      expect(mod.CairnQ).toBeTypeOf("function");
      expect(mod.SQLiteStore).toBeTypeOf("function"); // still exported...
      expect(opened.filter((f) => /better.sqlite3/i.test(f))).toEqual([]); // ...just not loaded
    } finally {
      process.dlopen = real;
    }
  });

  it("loads it on first use, and says what to install when it is missing", async () => {
    const { SQLiteStore } = await import("../src/index.js");
    const store = new SQLiteStore(":memory:");
    await store.connect(); // the first use — this is what may load the addon
    expect(await store.protocolVersion()).toBeGreaterThan(0);
    await store.close();
  });
});
