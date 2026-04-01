import { Panel } from "@newquantmodel/ui";

import type { DataHealthRecord, ForecastRecord, UniverseRecord } from "@newquantmodel/shared-types";

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
  reportPath,
  modelVersions
}: {
  generatedAt: string | null;
  universes: UniverseRecord[];
  dataHealth: DataHealthRecord[];
  forecasts: ForecastRecord[];
  reportPath: string | null;
  modelVersions: string[];
}) {
  const averageCoverage =
    dataHealth.length > 0 ? `${(dataHealth.reduce((sum, item) => sum + item.coveragePct, 0) / dataHealth.length).toFixed(1)}%` : "Pending";
  const tradableCoverage =
    dataHealth.length > 0 ? `${(dataHealth.reduce((sum, item) => sum + item.tradableCoveragePct, 0) / dataHealth.length).toFixed(1)}%` : "Pending";
  const topSignals = [...forecasts].sort((left, right) => right.expectedReturn - left.expectedReturn).slice(0, 6);
  const staleMarkets = dataHealth.filter((item) => item.stale).length;
  const publishedTime = formatDualTime(generatedAt);
  const reportPathDisplay = formatCompactPath(reportPath);
  const modelChips = modelVersions.map((item) => {
    const version = formatModelVersion(item);
    return { label: version.primary, title: version.secondary ?? item };
  });

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
          <MetricCard label="Stale Markets" value={String(staleMarkets)} />
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
              <span>{formatPercent(item.expectedReturn, 2)} expected / {(item.confidence * 100).toFixed(0)}% confidence</span>
              <span>{item.regime}</span>
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
