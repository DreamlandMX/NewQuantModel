import Fastify from "fastify";
import cors from "@fastify/cors";

import { getConfig } from "./lib/config.js";
import { registerRoutes } from "./routes/index.js";
import { JobRunner } from "./services/job-runner.js";
import { PublishedStore } from "./services/published-store.js";

async function main() {
  const config = getConfig();
  const app = Fastify({ logger: true });
  const store = new PublishedStore(config.publishedDataDir);
  const jobs = new JobRunner(config.appRoot, store);

  await app.register(cors, { origin: true });
  await registerRoutes(app, store, jobs);

  await app.listen({ port: config.port, host: "0.0.0.0" });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
