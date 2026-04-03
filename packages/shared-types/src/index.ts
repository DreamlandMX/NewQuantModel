import { z } from "zod";

export const marketSchema = z.enum(["crypto", "cn_equity", "us_equity", "index"]);
export type Market = z.infer<typeof marketSchema>;

export const strategyModeSchema = z.enum(["long_only", "hedged"]);
export type StrategyMode = z.infer<typeof strategyModeSchema>;

export const rebalanceFrequencySchema = z.enum(["intraday", "daily", "weekly"]);
export type RebalanceFrequency = z.infer<typeof rebalanceFrequencySchema>;

export const tradeSideSchema = z.enum(["long", "short"]);
export type TradeSide = z.infer<typeof tradeSideSchema>;

export const tradePlanStatusSchema = z.enum(["actionable", "filtered", "expired", "stale"]);
export type TradePlanStatus = z.infer<typeof tradePlanStatusSchema>;

export const jobTypeSchema = z.enum(["ingest", "feature", "train", "backtest", "publish", "report"]);
export type JobType = z.infer<typeof jobTypeSchema>;

export const jobStatusSchema = z.enum(["queued", "running", "completed", "failed"]);
export type JobStatus = z.infer<typeof jobStatusSchema>;

export const universeRecordSchema = z.object({
  market: marketSchema,
  universe: z.string(),
  coverageDate: z.string(),
  memberCount: z.number(),
  policyNotes: z.array(z.string()),
  tradableProxy: z.string().nullable(),
  dataSource: z.string(),
  coverageMode: z.enum(["approx_bootstrap", "point_in_time"]),
  historyStartDate: z.string(),
  coveragePct: z.number(),
  refreshSchedule: z.string(),
  lastRefreshAt: z.string(),
  stale: z.boolean(),
  publishedAt: z.string(),
  dataSnapshotVersion: z.string()
});
export type UniverseRecord = z.infer<typeof universeRecordSchema>;

export const assetRecordSchema = z.object({
  symbol: z.string(),
  name: z.string(),
  market: marketSchema,
  timezone: z.string(),
  isTradable: z.boolean(),
  hedgeProxy: z.string().nullable(),
  memberships: z.array(z.string()),
  riskBucket: z.string(),
  primaryVenue: z.string(),
  tradableSymbol: z.string().nullable(),
  quoteAsset: z.string().nullable(),
  hasPerpetualProxy: z.boolean(),
  historyCoverageStart: z.string(),
  sortRank: z.number(),
  sortMetric: z.number().nullable(),
  sortMetricLabel: z.string(),
  stale: z.boolean(),
  publishedAt: z.string(),
  dataSnapshotVersion: z.string()
});
export type AssetRecord = z.infer<typeof assetRecordSchema>;

export const forecastRecordSchema = z.object({
  symbol: z.string(),
  market: marketSchema,
  universe: z.string(),
  horizon: z.string(),
  pUp: z.number(),
  expectedReturn: z.number(),
  q10: z.number(),
  q50: z.number(),
  q90: z.number(),
  alphaScore: z.number(),
  confidence: z.number(),
  indicatorUnavailable: z.boolean(),
  macdLine: z.number(),
  macdSignal: z.number(),
  macdHist: z.number(),
  macdState: z.string(),
  rsi14: z.number(),
  rsiState: z.string(),
  atr14: z.number(),
  atrPct: z.number(),
  bbUpper: z.number(),
  bbMid: z.number(),
  bbLower: z.number(),
  bbWidth: z.number(),
  bbPosition: z.number(),
  bbState: z.string(),
  kValue: z.number(),
  dValue: z.number(),
  jValue: z.number(),
  kdjState: z.string(),
  regime: z.string(),
  riskFlags: z.array(z.string()),
  modelVersion: z.string(),
  asOfDate: z.string(),
  signalFrequency: rebalanceFrequencySchema,
  sourceFrequency: rebalanceFrequencySchema,
  isDerivedSignal: z.boolean(),
  forecastValidity: z.enum(["valid", "conflict", "adjusted"]).default("valid"),
  forecastConflictReason: z.string().nullable().default(null),
  forecastAdjusted: z.boolean().default(false),
  publishedAt: z.string(),
  dataSnapshotVersion: z.string(),
  stale: z.boolean(),
  coverageMode: z.enum(["approx_bootstrap", "point_in_time"]),
  coveragePct: z.number()
});
export type ForecastRecord = z.infer<typeof forecastRecordSchema>;

export const rankingRecordSchema = z.object({
  symbol: z.string(),
  universe: z.string(),
  rebalanceFreq: rebalanceFrequencySchema,
  strategyMode: strategyModeSchema,
  score: z.number(),
  rank: z.number(),
  expectedReturn: z.number(),
  targetWeight: z.number(),
  liquidityBucket: z.string(),
  factorExposures: z.record(z.number()),
  signalFamily: z.string(),
  signalBreakdown: z.record(z.number()),
  asOfDate: z.string(),
  modelVersion: z.string(),
  signalFrequency: rebalanceFrequencySchema,
  sourceFrequency: rebalanceFrequencySchema,
  isDerivedSignal: z.boolean(),
  forecastValidity: z.enum(["valid", "conflict", "adjusted"]).default("valid"),
  forecastConflictReason: z.string().nullable().default(null),
  forecastAdjusted: z.boolean().default(false),
  priceBasis: z.string().default("asset_spot"),
  executionBasis: z.string().default("native_market"),
  publishedAt: z.string(),
  dataSnapshotVersion: z.string(),
  stale: z.boolean(),
  coverageMode: z.enum(["approx_bootstrap", "point_in_time"]),
  coveragePct: z.number()
});
export type RankingRecord = z.infer<typeof rankingRecordSchema>;

export const tradePlanRecordSchema = z.object({
  symbol: z.string(),
  market: marketSchema,
  universe: z.string(),
  horizon: z.string(),
  strategyMode: strategyModeSchema,
  rebalanceFreq: rebalanceFrequencySchema,
  side: tradeSideSchema,
  entryBasis: z.literal("next_bar_open"),
  entryPriceMode: z.literal("planned_last_close_proxy"),
  entryPrice: z.number(),
  stopLossPrice: z.number(),
  takeProfitPrice: z.number(),
  riskPct: z.number(),
  rewardPct: z.number(),
  riskRewardRatio: z.number(),
  expectedReturn: z.number(),
  pUp: z.number(),
  confidence: z.number(),
  directionProbability: z.number(),
  tradeConfidence: z.number(),
  srUnavailable: z.boolean(),
  setupType: z.string(),
  levelRegime: z.string(),
  nearestSupport: z.number(),
  nearestResistance: z.number(),
  supportDistancePct: z.number(),
  resistanceDistancePct: z.number(),
  levelStrengthSupport: z.number(),
  levelStrengthResistance: z.number(),
  entrySource: z.string(),
  stopSource: z.string(),
  targetSource: z.string(),
  indicatorUnavailable: z.boolean(),
  macdLine: z.number(),
  macdSignal: z.number(),
  macdHist: z.number(),
  macdState: z.string(),
  rsi14: z.number(),
  rsiState: z.string(),
  atr14: z.number(),
  atrPct: z.number(),
  bbUpper: z.number(),
  bbMid: z.number(),
  bbLower: z.number(),
  bbWidth: z.number(),
  bbPosition: z.number(),
  bbState: z.string(),
  kValue: z.number(),
  dValue: z.number(),
  jValue: z.number(),
  kdjState: z.string(),
  indicatorAlignmentScore: z.number(),
  indicatorNotes: z.string(),
  actionable: z.boolean(),
  rejectionReason: z.string().nullable(),
  selectionRank: z.number(),
  selectionReason: z.string(),
  conflictGroupKey: z.string(),
  executionSymbol: z.string().nullable(),
  executionMode: z.string(),
  expiresAt: z.string(),
  modelVersion: z.string(),
  asOfDate: z.string(),
  signalFrequency: rebalanceFrequencySchema,
  sourceFrequency: rebalanceFrequencySchema,
  isDerivedSignal: z.boolean(),
  publishedAt: z.string(),
  dataSnapshotVersion: z.string(),
  stale: z.boolean(),
  coverageMode: z.enum(["approx_bootstrap", "point_in_time"]),
  coveragePct: z.number(),
  sortRank: z.number(),
  sortMetric: z.number().nullable(),
  sortMetricLabel: z.string(),
  snapshotPrice: z.number(),
  livePrice: z.number().nullable(),
  liveUpdatedAt: z.string().nullable(),
  priceSource: z.string().nullable(),
  priceDriftPct: z.number().nullable(),
  quoteStale: z.boolean(),
  runtimeFlags: z.array(z.string()),
  status: tradePlanStatusSchema,
  isExpired: z.boolean(),
  isBlocked: z.boolean(),
  blockedReason: z.string().nullable(),
  evaluatedAt: z.string()
});
export type TradePlanRecord = z.infer<typeof tradePlanRecordSchema>;

export const costStressRecordSchema = z.object({
  label: z.string(),
  sharpe: z.number(),
  maxDrawdown: z.number(),
  cagr: z.number()
});
export type CostStressRecord = z.infer<typeof costStressRecordSchema>;

export const backtestSummarySchema = z.object({
  strategyId: z.string(),
  rebalanceFreq: rebalanceFrequencySchema,
  strategyMode: strategyModeSchema,
  cagr: z.number(),
  sharpe: z.number(),
  maxDrawdown: z.number(),
  turnover: z.number(),
  hitRate: z.number(),
  ic: z.number(),
  rankIc: z.number(),
  topDecileSpread: z.number(),
  costStress: z.array(costStressRecordSchema),
  modelVersion: z.string(),
  signalFrequency: rebalanceFrequencySchema,
  sourceFrequency: rebalanceFrequencySchema,
  isDerivedSignal: z.boolean(),
  publishedAt: z.string(),
  dataSnapshotVersion: z.string(),
  stale: z.boolean(),
  benchmark: z.string().nullable()
});
export type BacktestSummary = z.infer<typeof backtestSummarySchema>;

export const jobStageRecordSchema = z.object({
  name: z.string(),
  status: jobStatusSchema,
  updatedAt: z.string(),
  message: z.string(),
  outputPath: z.string().nullable()
});
export type JobStageRecord = z.infer<typeof jobStageRecordSchema>;

export const jobRecordSchema = z.object({
  id: z.string(),
  type: jobTypeSchema,
  status: jobStatusSchema,
  requestedAt: z.string(),
  updatedAt: z.string(),
  message: z.string(),
  outputPath: z.string().nullable(),
  currentStage: z.string().nullable(),
  stages: z.array(jobStageRecordSchema),
  lastError: z.string().nullable()
});
export type JobRecord = z.infer<typeof jobRecordSchema>;

export const forecastResponseSchema = z.object({
  items: z.array(forecastRecordSchema)
});
export type ForecastResponse = z.infer<typeof forecastResponseSchema>;

export const rankingResponseSchema = z.object({
  items: z.array(rankingRecordSchema)
});
export type RankingResponse = z.infer<typeof rankingResponseSchema>;

export const tradePlanResponseSchema = z.object({
  items: z.array(tradePlanRecordSchema)
});
export type TradePlanResponse = z.infer<typeof tradePlanResponseSchema>;

export const liveQuoteRecordSchema = z.object({
  symbol: z.string(),
  market: marketSchema,
  lastPrice: z.number(),
  markPrice: z.number().nullable(),
  priceChangePct24h: z.number().nullable(),
  updatedAt: z.string(),
  source: z.string(),
  isStale: z.boolean()
});
export type LiveQuoteRecord = z.infer<typeof liveQuoteRecordSchema>;

export const liveQuoteResponseSchema = z.object({
  items: z.array(liveQuoteRecordSchema)
});
export type LiveQuoteResponse = z.infer<typeof liveQuoteResponseSchema>;

export const universesResponseSchema = z.object({
  items: z.array(universeRecordSchema)
});
export type UniversesResponse = z.infer<typeof universesResponseSchema>;

export const jobsResponseSchema = z.object({
  items: z.array(jobRecordSchema)
});
export type JobsResponse = z.infer<typeof jobsResponseSchema>;

export const dataHealthRecordSchema = z.object({
  market: marketSchema,
  lastRefreshAt: z.string(),
  coveragePct: z.number(),
  missingBarPct: z.number(),
  tradableCoveragePct: z.number(),
  membershipMode: z.enum(["approx_bootstrap", "point_in_time"]),
  historyStartDate: z.string(),
  stale: z.boolean(),
  notes: z.array(z.string()),
  publishedAt: z.string(),
  dataSnapshotVersion: z.string()
});
export type DataHealthRecord = z.infer<typeof dataHealthRecordSchema>;

export const dataHealthResponseSchema = z.object({
  items: z.array(dataHealthRecordSchema)
});
export type DataHealthResponse = z.infer<typeof dataHealthResponseSchema>;

export const reportManifestSchema = z.object({
  markdownPath: z.string(),
  csvPaths: z.array(z.string()),
  pdfPath: z.string(),
  generatedAt: z.string()
});
export type ReportManifest = z.infer<typeof reportManifestSchema>;
