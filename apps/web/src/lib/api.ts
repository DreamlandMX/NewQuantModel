import type {
  AssetRecord,
  BacktestSummary,
  DataHealthResponse,
  ForecastResponse,
  JobRecord,
  RankingResponse,
  ReportManifest,
  TradePlanResponse,
  UniversesResponse
} from "@newquantmodel/shared-types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:4000";

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`);
  if (!response.ok) {
    throw new Error(`Request failed for ${path}`);
  }
  return response.json() as Promise<T>;
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json"
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(`Request failed for ${path}`);
  }
  return response.json() as Promise<T>;
}

export const api = {
  health: () => getJson<{ ok: boolean; service: string; summary: { generatedAt: string | null; universes: number; forecasts: number; rankings: number; jobs: number; dataHealth: number } }>("/health"),
  universes: () => getJson<UniversesResponse>("/api/universes"),
  assets: () => getJson<{ items: AssetRecord[] }>("/api/assets"),
  dataHealth: () => getJson<DataHealthResponse>("/api/health/data"),
  forecasts: (query = "") => getJson<ForecastResponse>(`/api/forecasts${query}`),
  rankings: (query = "") => getJson<RankingResponse>(`/api/rankings${query}`),
  tradePlans: (query = "") => getJson<TradePlanResponse>(`/api/trade-plans${query}`),
  asset: (symbol: string) => getJson<AssetRecord>(`/api/assets/${encodeURIComponent(symbol)}`),
  backtest: (strategyId: string) => getJson<BacktestSummary>(`/api/backtests/${encodeURIComponent(strategyId)}`),
  jobs: () => getJson<{ items: JobRecord[] }>("/api/jobs"),
  runJob: (type: string) => postJson<JobRecord>("/api/jobs/run", { type }),
  latestReport: () => getJson<ReportManifest>("/api/reports/latest")
};
