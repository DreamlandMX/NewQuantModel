from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Literal, Optional


Market = Literal["crypto", "cn_equity", "us_equity", "index"]
StrategyMode = Literal["long_only", "hedged"]
RebalanceFrequency = Literal["daily", "weekly"]
TradeSide = Literal["long", "short"]
JobType = Literal["ingest", "feature", "train", "backtest", "publish", "report"]
JobStatus = Literal["queued", "running", "completed", "failed"]


@dataclass(slots=True)
class UniverseRecord:
    market: Market
    universe: str
    coverageDate: str
    memberCount: int
    policyNotes: List[str]
    tradableProxy: Optional[str]
    dataSource: str
    coverageMode: Literal["approx_bootstrap", "point_in_time"]
    historyStartDate: str
    coveragePct: float
    refreshSchedule: str
    lastRefreshAt: str
    stale: bool = False
    publishedAt: str = ""
    dataSnapshotVersion: str = ""


@dataclass(slots=True)
class AssetRecord:
    symbol: str
    name: str
    market: Market
    timezone: str
    isTradable: bool
    memberships: List[str]
    riskBucket: str
    hedgeProxy: Optional[str] = None
    primaryVenue: str = ""
    tradableSymbol: Optional[str] = None
    quoteAsset: Optional[str] = None
    hasPerpetualProxy: bool = False
    historyCoverageStart: str = ""
    stale: bool = False
    publishedAt: str = ""
    dataSnapshotVersion: str = ""


@dataclass(slots=True)
class ForecastRecord:
    symbol: str
    market: Market
    universe: str
    horizon: str
    pUp: float
    expectedReturn: float
    q10: float
    q50: float
    q90: float
    alphaScore: float
    confidence: float
    regime: str
    riskFlags: List[str]
    modelVersion: str
    asOfDate: str
    publishedAt: str = ""
    dataSnapshotVersion: str = ""
    stale: bool = False
    coverageMode: Literal["approx_bootstrap", "point_in_time"] = "point_in_time"
    coveragePct: float = 100.0


@dataclass(slots=True)
class RankingRecord:
    symbol: str
    universe: str
    rebalanceFreq: RebalanceFrequency
    strategyMode: StrategyMode
    score: float
    rank: int
    expectedReturn: float
    targetWeight: float
    liquidityBucket: str
    factorExposures: Dict[str, float]
    signalFamily: str
    signalBreakdown: Dict[str, float]
    asOfDate: str
    modelVersion: str = ""
    publishedAt: str = ""
    dataSnapshotVersion: str = ""
    stale: bool = False
    coverageMode: Literal["approx_bootstrap", "point_in_time"] = "point_in_time"
    coveragePct: float = 100.0


@dataclass(slots=True)
class TradePlanRecord:
    symbol: str
    market: Market
    universe: str
    horizon: str
    strategyMode: StrategyMode
    rebalanceFreq: RebalanceFrequency
    side: TradeSide
    entryBasis: Literal["next_bar_open"]
    entryPriceMode: Literal["planned_last_close_proxy"]
    entryPrice: float
    stopLossPrice: float
    takeProfitPrice: float
    riskPct: float
    rewardPct: float
    riskRewardRatio: float
    expectedReturn: float
    pUp: float
    confidence: float
    actionable: bool
    rejectionReason: Optional[str]
    executionSymbol: Optional[str]
    executionMode: str
    expiresAt: str
    modelVersion: str
    asOfDate: str
    publishedAt: str = ""
    dataSnapshotVersion: str = ""
    stale: bool = False
    coverageMode: Literal["approx_bootstrap", "point_in_time"] = "point_in_time"
    coveragePct: float = 100.0


@dataclass(slots=True)
class CostStressRecord:
    label: str
    sharpe: float
    maxDrawdown: float
    cagr: float


@dataclass(slots=True)
class BacktestSummary:
    strategyId: str
    rebalanceFreq: RebalanceFrequency
    strategyMode: StrategyMode
    cagr: float
    sharpe: float
    maxDrawdown: float
    turnover: float
    hitRate: float
    ic: float
    rankIc: float
    topDecileSpread: float
    costStress: List[CostStressRecord]
    modelVersion: str = ""
    publishedAt: str = ""
    dataSnapshotVersion: str = ""
    stale: bool = False
    benchmark: Optional[str] = None


@dataclass(slots=True)
class JobStageRecord:
    name: str
    status: JobStatus
    updatedAt: str
    message: str
    outputPath: Optional[str] = None


@dataclass(slots=True)
class JobRecord:
    id: str
    type: JobType
    status: JobStatus
    requestedAt: str
    updatedAt: str
    message: str
    outputPath: Optional[str] = None
    currentStage: Optional[str] = None
    stages: List[JobStageRecord] = field(default_factory=list)
    lastError: Optional[str] = None


@dataclass(slots=True)
class ReportManifest:
    markdownPath: str
    csvPaths: List[str]
    pdfPath: str
    generatedAt: str


@dataclass(slots=True)
class DataHealthRecord:
    market: Market
    lastRefreshAt: str
    coveragePct: float
    missingBarPct: float
    tradableCoveragePct: float
    membershipMode: Literal["approx_bootstrap", "point_in_time"]
    historyStartDate: str
    stale: bool
    notes: List[str]
    publishedAt: str = ""
    dataSnapshotVersion: str = ""


def to_dict(item: object) -> dict:
    return asdict(item)
