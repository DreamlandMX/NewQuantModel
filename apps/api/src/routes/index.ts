import type { FastifyInstance } from "fastify";

import { jobTypeSchema } from "@newquantmodel/shared-types";

import type { JobRunner } from "../services/job-runner.js";
import type { PublishedStore } from "../services/published-store.js";

export async function registerRoutes(app: FastifyInstance, store: PublishedStore, jobs: JobRunner) {
  app.get("/health", async () => ({
    ok: true,
    service: "newquantmodel-api",
    summary: await store.getHealthSummary()
  }));

  app.get("/api/universes", async () => store.getUniverses());
  app.get("/api/health/data", async () => store.getDataHealth());
  app.get("/api/assets", async () => store.getAssets());

  app.get("/api/assets/:symbol", async (request, reply) => {
    const params = request.params as { symbol: string };
    const item = await store.getAsset(params.symbol);
    if (!item) {
      return reply.code(404).send({ message: `Unknown asset ${params.symbol}` });
    }
    return item;
  });

  app.get("/api/live/quotes", async (request) => {
    const query = request.query as { market?: string; symbols?: string };
    const symbols = query.symbols
      ? query.symbols
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean)
      : undefined;
    return store.getLiveQuotes({ market: query.market, symbols });
  });

  app.get("/api/live/quotes/:symbol", async (request, reply) => {
    const params = request.params as { symbol: string };
    const item = store.getLiveQuote(params.symbol);
    if (!item) {
      return reply.code(404).send({ message: `Unknown live quote ${params.symbol}` });
    }
    return item;
  });

  app.get("/api/forecasts", async (request) => {
    const query = request.query as { market?: string; universe?: string; horizon?: string };
    return store.getForecasts(query);
  });

  app.get("/api/rankings", async (request) => {
    const query = request.query as { universe?: string; rebalanceFreq?: string; strategyMode?: string };
    return store.getRankings(query);
  });

  app.get("/api/trade-plans", async (request) => {
    const query = request.query as {
      market?: string;
      universe?: string;
      strategyMode?: string;
      rebalanceFreq?: string;
      side?: string;
      symbol?: string;
      actionableOnly?: string;
    };
    return store.getTradePlans(query);
  });

  app.get("/api/backtests/:strategyId", async (request, reply) => {
    const params = request.params as { strategyId: string };
    const item = await store.getBacktest(params.strategyId);
    if (!item) {
      return reply.code(404).send({ message: `Unknown strategy ${params.strategyId}` });
    }
    return item;
  });

  app.get("/api/jobs", async () => store.getJobs());

  app.get("/api/jobs/:id", async (request, reply) => {
    const params = request.params as { id: string };
    const jobsPayload = await store.getJobs();
    const job = jobsPayload.items.find((item) => item.id === params.id);
    if (!job) {
      return reply.code(404).send({ message: `Unknown job ${params.id}` });
    }
    return job;
  });

  app.post("/api/jobs/run", async (request, reply) => {
    const body = request.body as { type?: string };
    const parsed = jobTypeSchema.safeParse(body.type);
    if (!parsed.success) {
      return reply.code(400).send({ message: "Expected job type: ingest, feature, train, backtest, publish, or report." });
    }
    return jobs.enqueue(parsed.data);
  });

  app.get("/api/reports/latest", async (request, reply) => {
    const manifest = await store.getReportManifest();
    if (!manifest) {
      return reply.code(404).send({ message: "No published report manifest available." });
    }
    return manifest;
  });
}
