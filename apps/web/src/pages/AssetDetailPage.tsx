import { Panel } from "@newquantmodel/ui";

import type { AssetRecord, ForecastRecord, RankingRecord, TradePlanRecord } from "@newquantmodel/shared-types";

import { ValueBlock } from "../components/ValueBlock";
import {
  formatCoverageMode,
  formatDateOnly,
  formatDualTime,
  formatHorizon,
  formatModelVersion,
  formatPercent,
  formatPrice,
  formatRatio,
  formatRebalance,
  formatSide,
  formatStrategyMode,
  formatUniverseName
} from "../lib/formatters";

export function AssetDetailPage({
  asset,
  tradePlans,
  forecasts,
  rankings
}: {
  asset: AssetRecord | null;
  tradePlans: TradePlanRecord[];
  forecasts: ForecastRecord[];
  rankings: RankingRecord[];
}) {
  const publishedTime = formatDualTime(asset?.publishedAt);
  const sortedTradePlans = [...tradePlans].sort((left, right) => {
    if (Number(right.actionable) !== Number(left.actionable)) {
      return Number(right.actionable) - Number(left.actionable);
    }
    if (right.riskRewardRatio !== left.riskRewardRatio) {
      return right.riskRewardRatio - left.riskRewardRatio;
    }
    return right.confidence - left.confidence;
  });
  const actionablePlans = sortedTradePlans.filter((item) => item.actionable).slice(0, 8);
  const rejectedPlans = sortedTradePlans.filter((item) => !item.actionable).slice(0, 6);
  return (
    <Panel title="Asset Detail" eyebrow="Prediction + risk context">
      {asset ? (
        <div className="value-grid value-grid--details">
          <ValueBlock label="Symbol" primary={asset.symbol} secondary={asset.name} />
          <ValueBlock label="Memberships" primary={asset.memberships.map(formatUniverseName).join(", ")} secondary={asset.memberships.join(", ")} title={asset.memberships.join(", ")} />
          <ValueBlock label="Hedge Proxy" primary={asset.hedgeProxy ?? "None"} />
          <ValueBlock label="Primary Venue" primary={asset.primaryVenue} />
          <ValueBlock label="Tradable Symbol" primary={asset.tradableSymbol ?? "Research only"} />
          <ValueBlock label="Quote Asset" primary={asset.quoteAsset ?? "N/A"} />
          <ValueBlock label="Perpetual Proxy" primary={asset.hasPerpetualProxy ? "Available" : "Unavailable"} />
          <ValueBlock label="Timezone" primary={asset.timezone} />
          <ValueBlock label="Risk Bucket" primary={asset.riskBucket} />
          <ValueBlock label="History Coverage Start" primary={formatDateOnly(asset.historyCoverageStart)} secondary={asset.historyCoverageStart} title={asset.historyCoverageStart} />
          <ValueBlock label="Published At" primary={publishedTime.primary} secondary={publishedTime.secondary ?? undefined} title={publishedTime.title} />
          <ValueBlock label="Snapshot Version" primary={asset.dataSnapshotVersion} />
          <ValueBlock label="Asset Stale" primary={String(asset.stale)} />
        </div>
      ) : (
        <div className="empty-state">Publish sample data to inspect an asset.</div>
      )}
      {asset ? (
        <div className="jobs-list">
          {actionablePlans.length ? <span className="detail-label">Actionable Trade Plans</span> : null}
          {actionablePlans.map((item) => (
            <article className="job-row trade-plan-card" key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}-${item.horizon}-${item.side}`}>
              <strong>
                <span className={`side-pill side-pill--${item.side}`}>{formatSide(item.side)}</span>{" "}
                {formatHorizon(item.horizon)} / {formatUniverseName(item.universe)} / {formatStrategyMode(item.strategyMode)} / {formatRebalance(item.rebalanceFreq)}
              </strong>
              <span>Entry {formatPrice(item.entryPrice)} / SL {formatPrice(item.stopLossPrice)} / TP {formatPrice(item.takeProfitPrice)} / RR {formatRatio(item.riskRewardRatio)}</span>
              <span>{formatPercent(item.expectedReturn, 2)} expected / {formatPercent(item.confidence, 0)} confidence / expires {formatDualTime(item.expiresAt).primary}</span>
              <span>{item.executionSymbol ? `Execution ${item.executionSymbol}` : "No execution symbol"} / {item.entryBasis.replace(/_/g, " ")} / {item.entryPriceMode.replace(/_/g, " ")}</span>
            </article>
          ))}
          {rejectedPlans.length ? <span className="detail-label">Filtered Candidates</span> : null}
          {rejectedPlans.map((item) => (
            <article className="job-row trade-plan-card trade-plan-card--muted" key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}-${item.horizon}-${item.side}`}>
              <strong>
                <span className={`side-pill side-pill--${item.side}`}>{formatSide(item.side)}</span>{" "}
                {formatHorizon(item.horizon)} / {formatUniverseName(item.universe)} / {formatStrategyMode(item.strategyMode)} / {formatRebalance(item.rebalanceFreq)}
              </strong>
              <span>Entry {formatPrice(item.entryPrice)} / SL {formatPrice(item.stopLossPrice)} / TP {formatPrice(item.takeProfitPrice)} / RR {formatRatio(item.riskRewardRatio)}</span>
              <span>{item.rejectionReason ?? "Filtered"} / {formatPercent(item.confidence, 0)} confidence / expires {formatDualTime(item.expiresAt).primary}</span>
            </article>
          ))}
          {(actionablePlans.length || rejectedPlans.length) && <span className="detail-label">Forecasts</span>}
          {forecasts.map((item) => (
            <article className="job-row" key={`${item.symbol}-${item.universe}-${item.horizon}`}>
              <strong>{item.horizon} / {formatModelVersion(item.modelVersion).primary}</strong>
              <span>{formatPercent(item.expectedReturn, 2)} expected / {(item.pUp * 100).toFixed(0)}% up probability</span>
              <span>{formatPercent(item.q10, 2)} / {formatPercent(item.q50, 2)} / {formatPercent(item.q90, 2)}</span>
              <span>{item.regime} / {formatCoverageMode(item.coverageMode)} / {item.coveragePct.toFixed(1)}%</span>
            </article>
          ))}
          {rankings.length ? <span className="detail-label">Ranking Context</span> : null}
          {rankings.map((item) => (
            <article className="job-row" key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}`}>
              <strong>{formatUniverseName(item.universe)} / {formatStrategyMode(item.strategyMode)} / {formatRebalance(item.rebalanceFreq)}</strong>
              <span>score {item.score.toFixed(3)} / target {formatPercent(item.targetWeight, 2)}</span>
              <span>{formatModelVersion(item.modelVersion).primary}</span>
              <span>
                {Object.entries(item.signalBreakdown)
                  .filter(([, value]) => typeof value === "number" && Number.isFinite(value))
                  .map(([key, value]) => `${key}:${value.toFixed(2)}`)
                  .join(" | ")}
              </span>
            </article>
          ))}
        </div>
      ) : null}
    </Panel>
  );
}
