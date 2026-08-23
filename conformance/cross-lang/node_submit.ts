// Cross-language E2E: Node API submits and waits, prints `RESULT <json>`.
import { CairnQ } from "../../cairnq-node/src/index.js";

const [, , db, name, payloadJson] = process.argv;
const tasks = CairnQ.sqlite(db);
await tasks.connect();
try {
  const result = await tasks.call(name, JSON.parse(payloadJson), {
    timeoutMs: 15_000,
    pollMs: 50,
  });
  console.log("RESULT " + JSON.stringify(result));
} finally {
  await tasks.close();
}
