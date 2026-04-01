import fs from "node:fs/promises";
import path from "node:path";

import type {
  AssetRecord,
  BacktestSummary,
  DataHealthResponse,
  ForecastResponse,
  JobRecord,
  JobsResponse,
  RankingResponse,
  ReportManifest,
  TradePlanRecord,
  TradePlanResponse,
  UniverseRecord,
  UniversesResponse
} from "@newquantmodel/shared-types";

export class PublishedStore {
  constructor(private readonly publishedDataDir: string) {}

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

  getUniverses(): Promise<UniversesResponse> {
    return this.readJsonFile("universes.json", { items: [] satisfies UniverseRecord[] });
  }

  getAssets(): Promise<{ items: AssetRecord[] }> {
    return this.readJsonFile("assets.json", { items: [] satisfies AssetRecord[] });
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
    return {
      items: payload.items.filter((item) => {
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
        if (actionableOnly && !item.actionable) {
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
  }> {
    const [universes, forecasts, rankings, jobs, report, dataHealth] = await Promise.all([
      this.getUniverses(),
      this.getForecasts({}),
      this.getRankings({}),
      this.getJobs(),
      this.getReportManifest(),
      this.getDataHealth()
    ]);

    return {
      generatedAt: report?.generatedAt ?? null,
      universes: universes.items.length,
      forecasts: forecasts.items.length,
      rankings: rankings.items.length,
      jobs: jobs.items.length,
      dataHealth: dataHealth.items.length
    };
  }
}
