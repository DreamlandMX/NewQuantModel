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
        <div className="metric-grid metric-grid--overview">
          <MetricCard label="Published Snapshot" value={publishedTime.primary} secondary={publishedTime.secondary} title={publishedTime.title} wide />
          <MetricCard label="Universes" value={String(universes.length)} />
          <MetricCard label="Forecast Rows" value={String(forecasts.length)} />
          <MetricCard label="Coverage Mode" value="Batch Published" hint="TS online, Python offline" />
          <MetricCard label="Average Coverage" value={averageCoverage} />
          <MetricCard label="Tradable Coverage" value={tradableCoverage} />
          <MetricCard label="Actionable Plans" value={String(actionablePlans)} />
          <MetricCard label="Expired Plans" value={String(expiredPlans)} />
          <MetricCard label="Stale Markets" value={String(staleMarkets)} />
          <MetricCard
            label="Worker"
            value={scheduler?.workerStatus === "running" ? "Auto-refresh on" : "Worker stopped"}
            secondary={workerHeartbeat.primary}
            hint={scheduler?.lastError ?? `Polling every ${scheduler?.pollSeconds ?? 60}s`}
          />
          <MetricCard
            label="Crypto Live Quotes"
            value={liveConnected ? "Connected" : "Stale"}
            secondary={topLiveQuotes.length ? `${topLiveQuotes.length} tracked leaders` : "No live quotes yet"}
            hint="Realtime exchange overlay"
          />
          <MetricCard
            label="Index Live Quotes"
            value={indexLiveConnected ? "Connected" : "Stale"}
            secondary={topIndexQuotes.length ? `${topIndexQuotes.length} tracked benchmarks` : "No live quotes yet"}
            hint="Realtime index point overlay"
          />
          <article className="metric-card metric-card--wide">
            <span className="metric-card__label">Model Stack</span>
            <ChipList items={modelChips.length > 0 ? modelChips : [{ label: "Baseline Signals" }]} />
            <span className="metric-card__hint">Latest published versions</span>
          </article>
        </div>
      </Panel>
      <Panel title="Market Coverage" eyebrow="Freshness + PIT status">
        <div className="value-grid value-grid--status">
          {dataHealth.map((item) => (
            <ValueBlock
              key={`${item.market}-${item.lastRefreshAt}`}
              label={formatMarketName(item.market)}
              primary={`${formatPercent(item.coveragePct / 100, 1)} coverage`}
              secondary={`${formatPercent(item.tradableCoveragePct / 100, 1)} tradable / ${formatCoverageMode(item.membershipMode)}`}
              tertiary={`History from ${item.historyStartDate} / stale: ${String(item.stale)}`}
            />
          ))}
        </div>
      </Panel>
      <Panel title="Top Signals" eyebrow="Latest publish">
        <div className="jobs-list">
          {topSignals.map((item) => (
            <article className="job-row" key={`${item.symbol}-${item.universe}-${item.horizon}`}>
              <strong>{item.symbol} / {formatUniverseName(item.universe)}</strong>
              <span>{item.horizon} / {formatModelVersion(item.modelVersion).primary}</span>
              <span>{formatPercent(item.expectedReturn, 2)} expected / {formatPercent(item.directionProbability, 0)} direction probability / {formatPercent(item.tradeConfidence, 0)} trade confidence</span>
              <span>{item.side} / RR {(item.riskRewardRatio ?? 0).toFixed(2)}x</span>
            </article>
          ))}
        </div>
      </Panel>
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
              />
            );
          })}
          {topIndexQuotes.length === 0 ? <div className="empty-state">Live index quotes are warming up.</div> : null}
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
