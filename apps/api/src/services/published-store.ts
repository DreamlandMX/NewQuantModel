import fs from "node:fs/promises";
import path from "node:path";

import type {
  AssetRecord,
  BacktestSummary,
  DataHealthResponse,
  ForecastResponse,
  JobRecord,
  JobsResponse,
  LiveQuoteRecord,
  RankingResponse,
  ReportManifest,
  TradePlanRecord,
  TradePlanResponse,
  UniverseRecord,
  UniversesResponse
} from "@newquantmodel/shared-types";

import type { CryptoLiveQuoteService } from "./live-quote-service.js";

type SchedulerMarketState = {
  market: string;
  lastCompletedBucket: string | null;
  lastRunAt: string | null;
  lastSuccessAt: string | null;
  lastError: string | null;
  nextScheduledAt: string | null;
};

type SchedulerState = {
  worker: {
    heartbeatAt: string | null;
    pollSeconds: number | null;
    lastLoopAt: string | null;
    lastError: string | null;
    status: string;
  };
  markets: Record<string, SchedulerMarketState>;
};

export class PublishedStore {
  constructor(
    private readonly publishedDataDir: string,
    private readonly liveQuotes: CryptoLiveQuoteService | null = null
  ) {}

  private async readJsonFile<T>(name: string, fallback: T): Promise<T> {
    const target = path.join(this.publishedDataDir, name);
    try {
      const content = await fs.readFile(target, "utf8");
      return JSON.parse(content) as T;
    } catch {
      return fallback;
    }
  }

  private async writeJsonFile<T>(name: string, payload: T): Promise<void> {
    await fs.mkdir(this.publishedDataDir, { recursive: true });
    await fs.writeFile(path.join(this.publishedDataDir, name), JSON.stringify(payload, null, 2), "utf8");
  }

  private parseIsoTime(value: string | null | undefined): Date | null {
    if (!value) {
      return null;
    }
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  private evaluateTradePlan(item: TradePlanRecord, evaluatedAtIso = new Date().toISOString()): TradePlanRecord {
    const evaluatedAt = this.parseIsoTime(evaluatedAtIso) ?? new Date();
    const expiresAt = this.parseIsoTime(item.expiresAt);
    const isExpired = expiresAt ? evaluatedAt.getTime() > expiresAt.getTime() : false;
    const staleBlocked = Boolean(item.stale);

    let status: TradePlanRecord["status"] = "filtered";
    let blockedReason: string | null = item.rejectionReason ?? null;

    if (isExpired) {
      status = "expired";
      blockedReason = "expired_at_next_rebalance";
    } else if (staleBlocked) {
      status = "stale";
      blockedReason = "stale_market_or_universe_data";
    } else if (item.actionable) {
      status = "actionable";
      blockedReason = null;
    }

    const quoteSymbol = item.market === "index" ? item.symbol : item.executionSymbol ?? item.symbol;
    const hasLiveOverlay = item.market === "crypto" || item.market === "index";
    const liveQuote = hasLiveOverlay ? this.liveQuotes?.getQuote(quoteSymbol) ?? null : null;
    const snapshotPrice = item.entryPrice;
    const livePrice = liveQuote?.lastPrice ?? null;
    const priceDriftPct =
      livePrice !== null && Number.isFinite(snapshotPrice) && snapshotPrice !== 0 ? (livePrice - snapshotPrice) / snapshotPrice : null;
    const quoteStale = hasLiveOverlay ? liveQuote?.isStale ?? true : false;
    const runtimeFlags: string[] = [];

    if (hasLiveOverlay && quoteStale) {
      runtimeFlags.push("live_quote_stale");
    }
    if (hasLiveOverlay && !liveQuote) {
      runtimeFlags.push("live_quote_unavailable");
    }
    if (hasLiveOverlay && priceDriftPct !== null && Math.abs(priceDriftPct) >= Math.max(item.riskPct * 0.75, 0.01)) {
      runtimeFlags.push("price_far_from_entry");
    }
    if (
      hasLiveOverlay &&
      priceDriftPct !== null &&
      ((item.side === "long" && priceDriftPct >= Math.max(item.riskPct * 0.5, 0.005)) ||
        (item.side === "short" && priceDriftPct <= -Math.max(item.riskPct * 0.5, 0.005)))
    ) {
      runtimeFlags.push("entry_window_missed");
    }

    return {
      ...item,
      snapshotPrice,
      livePrice,
      liveUpdatedAt: liveQuote?.updatedAt ?? null,
      priceSource: liveQuote?.source ?? null,
      priceDriftPct,
      quoteStale,
      runtimeFlags,
      status,
      isExpired,
      isBlocked: isExpired || staleBlocked,
      blockedReason,
      evaluatedAt: evaluatedAt.toISOString()
    };
  }

  private async getSchedulerState(): Promise<SchedulerState> {
    const fallback: SchedulerState = {
      worker: {
        heartbeatAt: null,
        pollSeconds: null,
        lastLoopAt: null,
        lastError: null,
        status: "idle"
      },
      markets: {
        crypto: { market: "crypto", lastCompletedBucket: null, lastRunAt: null, lastSuccessAt: null, lastError: null, nextScheduledAt: null },
        cn_equity: { market: "cn_equity", lastCompletedBucket: null, lastRunAt: null, lastSuccessAt: null, lastError: null, nextScheduledAt: null },
        us_equity: { market: "us_equity", lastCompletedBucket: null, lastRunAt: null, lastSuccessAt: null, lastError: null, nextScheduledAt: null }
      }
    };
    const payload = await this.readJsonFile<Record<string, unknown>>(path.join("..", "reference", "scheduler_state.json"), fallback);
    if (payload && typeof payload === "object" && "worker" in payload && "markets" in payload) {
      return payload as typeof fallback;
    }

    const legacy = { ...fallback };
    for (const market of Object.keys(legacy.markets) as Array<keyof typeof legacy.markets>) {
      const bucket = (payload as Record<string, unknown> | null)?.[market];
      if (typeof bucket === "string") {
        legacy.markets[market].lastCompletedBucket = bucket;
      }
    }
    return legacy;
  }

  getUniverses(): Promise<UniversesResponse> {
    return this.readJsonFile("universes.json", { items: [] satisfies UniverseRecord[] });
  }

  getAssets(): Promise<{ items: AssetRecord[] }> {
    return this.readJsonFile("assets.json", { items: [] satisfies AssetRecord[] });
  }

  getLiveQuotes(params?: { market?: string; symbols?: string[] }): { items: LiveQuoteRecord[] } {
    return this.liveQuotes?.getQuotes(params) ?? { items: [] };
  }

  getLiveQuote(symbol: string): LiveQuoteRecord | null {
    return this.liveQuotes?.getQuote(symbol) ?? null;
  }

  async getAsset(symbol: string): Promise<AssetRecord | null> {
    const payload = await this.getAssets();
    return payload.items.find((item) => item.symbol.toLowerCase() === symbol.toLowerCase()) ?? null;
  }

  async getForecasts(params: { market?: string; universe?: string; horizon?: string }): Promise<ForecastResponse> {
    const payload = await this.readJsonFile<ForecastResponse>("forecasts.json", { items: [] });
    return {
      items: payload.items.filter((item) => {
        if (params.market && item.market !== params.market) {
          return false;
        }
        if (params.universe && item.universe !== params.universe) {
          return false;
        }
        if (params.horizon && item.horizon !== params.horizon) {
          return false;
        }
        return true;
      })
    };
  }

  async getRankings(params: { universe?: string; rebalanceFreq?: string; strategyMode?: string }): Promise<RankingResponse> {
    const payload = await this.readJsonFile<RankingResponse>("rankings.json", { items: [] });
    return {
      items: payload.items.filter((item) => {
        if (params.universe && item.universe !== params.universe) {
          return false;
        }
        if (params.rebalanceFreq && item.rebalanceFreq !== params.rebalanceFreq) {
          return false;
        }
        if (params.strategyMode && item.strategyMode !== params.strategyMode) {
          return false;
        }
        return true;
      })
    };
  }

  async getTradePlans(params: {
    market?: string;
    universe?: string;
    strategyMode?: string;
    rebalanceFreq?: string;
    side?: string;
    symbol?: string;
    actionableOnly?: string | boolean;
  }): Promise<TradePlanResponse> {
    const payload = await this.readJsonFile<TradePlanResponse>("trade-plans.json", { items: [] satisfies TradePlanRecord[] });
    const actionableOnly = params.actionableOnly === undefined ? true : `${params.actionableOnly}` !== "false";
    const evaluatedAt = new Date().toISOString();
    return {
      items: payload.items.map((item) => this.evaluateTradePlan(item, evaluatedAt)).filter((item) => {
        if (params.market && item.market !== params.market) {
          return false;
        }
        if (params.universe && item.universe !== params.universe) {
          return false;
        }
        if (params.strategyMode && item.strategyMode !== params.strategyMode) {
          return false;
        }
        if (params.rebalanceFreq && item.rebalanceFreq !== params.rebalanceFreq) {
          return false;
        }
        if (params.side && item.side !== params.side) {
          return false;
        }
        if (params.symbol && item.symbol.toLowerCase() !== params.symbol.toLowerCase()) {
          return false;
        }
        if (actionableOnly && item.status !== "actionable") {
          return false;
        }
        return true;
      })
    };
  }

  async getBacktest(strategyId: string): Promise<BacktestSummary | null> {
    const payload = await this.readJsonFile("backtests.json", { items: [] as BacktestSummary[] });
    return payload.items.find((item) => item.strategyId === strategyId) ?? null;
  }

  getJobs(): Promise<JobsResponse> {
    return this.readJsonFile("jobs.json", { items: [] as JobRecord[] });
  }

  async upsertJob(record: JobRecord): Promise<void> {
    const payload = await this.getJobs();
    const next = payload.items.filter((item) => item.id !== record.id);
    next.unshift(record);
    await this.writeJsonFile("jobs.json", { items: next.slice(0, 50) });
  }

  getReportManifest(): Promise<ReportManifest | null> {
    return this.readJsonFile("report-manifest.json", null);
  }

  getDataHealth(): Promise<DataHealthResponse> {
    return this.readJsonFile<DataHealthResponse>("data-health.json", { items: [] });
  }

  async getHealthSummary(): Promise<{
    generatedAt: string | null;
    universes: number;
    forecasts: number;
    rankings: number;
    jobs: number;
    dataHealth: number;
    scheduler: {
      workerStatus: "running" | "stopped";
      heartbeatAt: string | null;
      lastError: string | null;
      pollSeconds: number | null;
      markets: Array<{
        market: string;
        lastSuccessAt: string | null;
        nextScheduledAt: string | null;
        lastCompletedBucket: string | null;
        lastError: string | null;
      }>;
    };
    tradePlanCounts: Record<TradePlanRecord["status"], number>;
  }> {
    const [universes, forecasts, rankings, jobs, report, dataHealth, schedulerState, tradePlans] = await Promise.all([
      this.getUniverses(),
      this.getForecasts({}),
      this.getRankings({}),
      this.getJobs(),
      this.getReportManifest(),
      this.getDataHealth(),
      this.getSchedulerState(),
      this.getTradePlans({ actionableOnly: false })
    ]);

    const heartbeatAt = this.parseIsoTime(schedulerState.worker.heartbeatAt);
    const pollSeconds = schedulerState.worker.pollSeconds ?? 60;
    const workerIsRunning = heartbeatAt ? Date.now() - heartbeatAt.getTime() <= Math.max(pollSeconds * 3 * 1000, 1_800_000) : false;
    const statusCounts = tradePlans.items.reduce(
      (acc, item) => {
        acc[item.status] = (acc[item.status] ?? 0) + 1;
        return acc;
      },
      { actionable: 0, filtered: 0, expired: 0, stale: 0 } as Record<TradePlanRecord["status"], number>
    );

    return {
      generatedAt: report?.generatedAt ?? null,
      universes: universes.items.length,
      forecasts: forecasts.items.length,
      rankings: rankings.items.length,
      jobs: jobs.items.length,
      dataHealth: dataHealth.items.length,
      scheduler: {
        workerStatus: workerIsRunning ? "running" : "stopped",
        heartbeatAt: schedulerState.worker.heartbeatAt,
        lastError: schedulerState.worker.lastError,
        pollSeconds: schedulerState.worker.pollSeconds,
        markets: Object.values(schedulerState.markets)
      },
      tradePlanCounts: statusCounts
    };
  }
}
