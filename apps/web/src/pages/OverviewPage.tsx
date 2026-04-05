import { Panel } from "@newquantmodel/ui";

import type { DataHealthRecord, ForecastRecord, LiveQuoteRecord, TradePlanRecord, UniverseRecord } from "@newquantmodel/shared-types";

import { ForecastDistribution } from "../components/ForecastDistribution";
import { MetricCard } from "../components/MetricCard";
import { PriceTapeChart } from "../components/PriceTapeChart";
import { ChipList, ValueBlock } from "../components/ValueBlock";
import {
  formatCompactPath,
  formatCoverageMode,
  formatDualTime,
  formatMarketName,
  formatModelVersion,
  formatPercent,
  formatUniverseName
} from "../lib/formatters";

export function OverviewPage({
  generatedAt,
  universes,
  dataHealth,
  forecasts,
  tradePlans,
  liveQuotes,
  reportPath,
  modelVersions,
  scheduler
}: {
  generatedAt: string | null;
  universes: UniverseRecord[];
  dataHealth: DataHealthRecord[];
  forecasts: ForecastRecord[];
  tradePlans: TradePlanRecord[];
  liveQuotes: LiveQuoteRecord[];
  reportPath: string | null;
  modelVersions: string[];
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
  } | null;
}) {
  const averageCoverage =
    dataHealth.length > 0 ? `${(dataHealth.reduce((sum, item) => sum + item.coveragePct, 0) / dataHealth.length).toFixed(1)}%` : "Pending";
  const tradableCoverage =
    dataHealth.length > 0 ? `${(dataHealth.reduce((sum, item) => sum + item.tradableCoveragePct, 0) / dataHealth.length).toFixed(1)}%` : "Pending";
  const topSignals = [...tradePlans]
    .filter((item) => item.status === "actionable")
    .sort((left, right) => {
      if (right.tradeConfidence !== left.tradeConfidence) {
        return right.tradeConfidence - left.tradeConfidence;
      }
      if (right.riskRewardRatio !== left.riskRewardRatio) {
        return right.riskRewardRatio - left.riskRewardRatio;
      }
      return right.directionProbability - left.directionProbability;
    })
    .slice(0, 6);
  const staleMarkets = dataHealth.filter((item) => item.stale).length;
  const actionablePlans = tradePlans.filter((item) => item.status === "actionable").length;
  const expiredPlans = tradePlans.filter((item) => item.status === "expired").length;
  const publishedTime = formatDualTime(generatedAt);
  const workerHeartbeat = formatDualTime(scheduler?.heartbeatAt ?? null);
  const reportPathDisplay = formatCompactPath(reportPath);
  const modelChips = modelVersions.map((item) => {
    const version = formatModelVersion(item);
    return { label: version.primary, title: version.secondary ?? item };
  });
  const topLiveQuotes = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    .map((symbol) => liveQuotes.find((item) => item.symbol === symbol))
    .filter((item): item is LiveQuoteRecord => Boolean(item));
  const topIndexQuotes = ["^GSPC", "^NDX", "^DJI"]
    .map((symbol) => liveQuotes.find((item) => item.symbol === symbol))
    .filter((item): item is LiveQuoteRecord => Boolean(item));
  const liveConnected = topLiveQuotes.some((item) => !item.isStale);
  const indexLiveConnected = topIndexQuotes.some((item) => !item.isStale);

  return (
    <div className="page-grid">
      <Panel title="Platform State" eyebrow="Overview">
        <div className="overview-brief">
          <div className="overview-brief__hero">
            <span className="overview-brief__eyebrow">Market brief</span>
            <h3>{publishedTime.primary}</h3>
            <p>{publishedTime.secondary ?? "Awaiting first publish"}</p>
            <div className="overview-brief__meta">
              <span>Batch-published research snapshot</span>
              <span>{reportPathDisplay.primary}</span>
            </div>
          </div>
          <div className="overview-kpi-grid">
            <MetricCard label="Universes" value={String(universes.length)} compact />
            <MetricCard label="Forecast Rows" value={String(forecasts.length)} compact />
            <MetricCard label="Actionable Plans" value={String(actionablePlans)} tone={actionablePlans > 0 ? "positive" : "neutral"} compact />
            <MetricCard label="Stale Markets" value={String(staleMarkets)} tone={staleMarkets > 0 ? "negative" : "positive"} compact />
            <MetricCard label="Coverage" value={averageCoverage} secondary={`Tradable ${tradableCoverage}`} compact />
            <MetricCard label="Worker" value={scheduler?.workerStatus === "running" ? "Running" : "Stopped"} secondary={workerHeartbeat.primary} tone={scheduler?.workerStatus === "running" ? "positive" : "negative"} compact />
          </div>
        </div>
        <div className="overview-subgrid">
          <article className="market-module">
            <div className="market-module__header">
              <div>
                <span className="panel__eyebrow">Model stack</span>
                <h3>Published model lane</h3>
              </div>
              <strong>Batch Published</strong>
            </div>
            <ChipList items={modelChips.length > 0 ? modelChips : [{ label: "Baseline Signals" }]} />
            <p className="market-module__note">Production snapshot versions currently visible to the API and terminal.</p>
          </article>
          <article className="market-module">
            <div className="market-module__header">
              <div>
                <span className="panel__eyebrow">Execution state</span>
                <h3>System readiness</h3>
              </div>
              <strong className={liveConnected ? "text-positive" : "text-warning"}>{liveConnected ? "Live linked" : "Delayed"}</strong>
            </div>
            <div className="market-module__stats">
              <ValueBlock label="Crypto Tape" primary={liveConnected ? "Connected" : "Stale"} secondary={topLiveQuotes.length ? `${topLiveQuotes.length} leaders tracked` : "No leaders"} tone={liveConnected ? "positive" : "muted"} />
              <ValueBlock label="Index Tape" primary={indexLiveConnected ? "Connected" : "Stale"} secondary={topIndexQuotes.length ? `${topIndexQuotes.length} benchmarks tracked` : "No benchmarks"} tone={indexLiveConnected ? "positive" : "muted"} />
              <ValueBlock label="Research Pack" primary={reportPathDisplay.primary} secondary={reportPathDisplay.secondary ?? "No latest export"} tone="accent" />
            </div>
          </article>
        </div>
      </Panel>
      <Panel title="Market Coverage" eyebrow="Freshness + PIT status">
        <div className="market-health-grid">
          {dataHealth.map((item) => (
            <ValueBlock
              key={`${item.market}-${item.lastRefreshAt}`}
              label={formatMarketName(item.market)}
              primary={`${formatPercent(item.coveragePct / 100, 1)} coverage`}
              secondary={`${formatPercent(item.tradableCoveragePct / 100, 1)} tradable / ${formatCoverageMode(item.membershipMode)}`}
              tertiary={`History from ${item.historyStartDate} / stale: ${String(item.stale)}`}
              tone={item.stale ? "negative" : "accent"}
            />
          ))}
        </div>
      </Panel>
      <div className="overview-market-row">
        <Panel title="Crypto Live Tape" eyebrow="Realtime price overlay">
          <div className="value-grid value-grid--status">
            {topLiveQuotes.map((item) => {
              const updated = formatDualTime(item.updatedAt);
              return (
                <ValueBlock
                  key={item.symbol}
                  label={item.symbol}
                  primary={item.lastPrice.toLocaleString("en-US", { maximumFractionDigits: item.lastPrice >= 1000 ? 2 : 4 })}
                  secondary={item.markPrice ? `Mark ${item.markPrice.toLocaleString("en-US", { maximumFractionDigits: item.markPrice >= 1000 ? 2 : 4 })} / 24h ${formatPercent(item.priceChangePct24h ?? 0, 2)}` : `24h ${formatPercent(item.priceChangePct24h ?? 0, 2)}`}
                  tertiary={`${item.isStale ? "stale" : "live"} / ${updated.primary}`}
                  title={updated.title}
                  className="value-block--quote"
                  tone={item.isStale ? "muted" : "accent"}
                />
              );
            })}
            {topLiveQuotes.length === 0 ? <div className="empty-state">Live crypto quotes are warming up.</div> : null}
          </div>
        </Panel>
        <Panel title="Index Live Tape" eyebrow="Realtime index overlay">
          <div className="value-grid value-grid--status">
            {topIndexQuotes.map((item) => {
              const updated = formatDualTime(item.updatedAt);
              return (
                <ValueBlock
                  key={item.symbol}
                  label={item.symbol}
                  primary={item.lastPrice.toLocaleString("en-US", { maximumFractionDigits: 2 })}
                  secondary={`24h ${formatPercent(item.priceChangePct24h ?? 0, 2)}`}
                  tertiary={`${item.isStale ? "stale" : "live"} / ${updated.primary}`}
                  title={updated.title}
                  className="value-block--quote"
                  tone={item.isStale ? "muted" : "accent"}
                />
              );
            })}
            {topIndexQuotes.length === 0 ? <div className="empty-state">Live index quotes are warming up.</div> : null}
          </div>
        </Panel>
      </div>
      <Panel title="Top Signals" eyebrow="Latest publish">
        <div className="signal-tile-grid">
          {topSignals.map((item) => (
            <article className="signal-tile" key={`${item.symbol}-${item.universe}-${item.horizon}`}>
              <div className="signal-tile__header">
                <strong>{item.symbol}</strong>
                <span className={`side-pill side-pill--${item.side}`}>{item.side}</span>
              </div>
              <div className="signal-tile__body">
                <span>{formatUniverseName(item.universe)}</span>
                <span>{item.horizon} / {formatModelVersion(item.modelVersion).primary}</span>
              </div>
              <div className="signal-tile__metrics">
                <span>{formatPercent(item.expectedReturn, 2)} exp.</span>
                <span>{formatPercent(item.directionProbability, 0)} dir.</span>
                <span>{formatPercent(item.tradeConfidence, 0)} conf.</span>
                <span>RR {(item.riskRewardRatio ?? 0).toFixed(2)}x</span>
              </div>
            </article>
          ))}
        </div>
      </Panel>
      <Panel title="Return Distribution" eyebrow="Cross-market">
        <ForecastDistribution items={forecasts} />
      </Panel>
      <Panel title="Price Tape" eyebrow="Median trajectory">
        <PriceTapeChart items={forecasts} />
      </Panel>
      <Panel title="Research Report" eyebrow="Latest export">
        <ValueBlock
          label="PDF report"
          primary={reportPathDisplay.primary}
          secondary={reportPathDisplay.secondary ?? "No report published yet"}
          title={reportPathDisplay.title}
          className="value-block--report"
        />
      </Panel>
    </div>
  );
}
