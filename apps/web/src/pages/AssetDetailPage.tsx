import { Panel } from "@newquantmodel/ui";

import type { AssetRecord, ForecastRecord, LiveQuoteRecord, RankingRecord, TradePlanRecord } from "@newquantmodel/shared-types";

import { ValueBlock } from "../components/ValueBlock";
import {
  formatBlockedReason,
  formatCoverageMode,
  formatDateOnly,
  formatDualTime,
  formatHorizon,
  formatIndicatorSummary,
  formatLevelRegime,
  formatModelVersion,
  formatPercent,
  formatPrice,
  formatRatio,
  formatRebalance,
  formatSignedPercent,
  formatSignalProvenance,
  formatSide,
  formatSortMetric,
  formatSetupType,
  formatStrategyMode,
  formatTradePlanStatus,
  formatUniverseName,
  horizonSortValue,
  humanizeToken,
  formatValidityState,
  formatRelativeTime
} from "../lib/formatters";

function topBreakdownEntries(signalBreakdown: Record<string, number>) {
  return Object.entries(signalBreakdown)
    .filter(([, value]) => typeof value === "number" && Number.isFinite(value))
    .sort((left, right) => Math.abs(right[1]) - Math.abs(left[1]))
    .slice(0, 6)
    .map(([key, value]) => `${key} ${value >= 0 ? "+" : ""}${value.toFixed(2)}`);
}

function PlanTicket({
  item,
  muted = false
}: {
  item: TradePlanRecord;
  muted?: boolean;
}) {
  const extended = item as TradePlanRecord & {
    forecastConflictReason?: string | null;
    priceBasis?: string;
    executionBasis?: string;
  };
  const signal = formatSignalProvenance(item.signalFrequency, item.sourceFrequency, item.isDerivedSignal, item.horizon);
  const liveDrift = item.priceDriftPct === null ? "N/A" : formatSignedPercent(item.priceDriftPct, 2);
  const blockedReason = formatBlockedReason(item.blockedReason ?? item.rejectionReason ?? extended.forecastConflictReason);
  const validity = formatValidityState(item);

  return (
    <article className={muted ? "ticket-card ticket-card--muted" : "ticket-card"}>
      <div className="ticket-card__header">
        <div>
          <strong>
            <span className={`side-pill side-pill--${item.side}`}>{formatSide(item.side)}</span>{" "}
            {formatHorizon(item.horizon)} / {formatUniverseName(item.universe)}
          </strong>
          <span>{formatRebalance(item.rebalanceFreq)} / {signal.primary}</span>
        </div>
        <div className="ticket-card__status">
          <strong className={`status-pill status-pill--${item.status}`}>{formatTradePlanStatus(item.status)}</strong>
          <strong className={`validity-pill validity-pill--${validity.tone}`}>{validity.label}</strong>
        </div>
      </div>
      <div className="ticket-card__grid">
        <span><strong>Entry</strong> {formatPrice(item.entryPrice)}</span>
        <span><strong>Stop</strong> {formatPrice(item.stopLossPrice)}</span>
        <span><strong>Target</strong> {formatPrice(item.takeProfitPrice)}</span>
        <span><strong>RR</strong> {formatRatio(item.riskRewardRatio)}</span>
        <span><strong>Live</strong> {item.livePrice === null ? "Unavailable" : formatPrice(item.livePrice)}</span>
        <span><strong>Drift</strong> {liveDrift}</span>
        <span><strong>Dir</strong> {formatPercent(item.directionProbability, 0)}</span>
        <span><strong>Conf</strong> {formatPercent(item.tradeConfidence, 0)}</span>
      </div>
      <div className="ticket-card__notes">
        <span><strong>Structure</strong> {formatSetupType(item.setupType)} / {formatLevelRegime(item.levelRegime)}</span>
        <span><strong>Levels</strong> S {formatPrice(item.nearestSupport)} / R {formatPrice(item.nearestResistance)}</span>
        <span><strong>Execution</strong> {item.executionSymbol ?? humanizeToken(item.executionMode)} / {extended.priceBasis ?? "asset_spot"} / {extended.executionBasis ?? "native_market"}</span>
        <span><strong>Gate</strong> {muted ? blockedReason : formatIndicatorSummary(item.indicatorNotes)}</span>
        <span><strong>Validity</strong> until {formatRelativeTime(item.validUntil)} / next bar {formatDualTime(item.nextBarAt).primary}</span>
      </div>
    </article>
  );
}

function TechnicalIndicatorCard({ item }: { item: ForecastRecord }) {
  return (
    <article className="indicator-card">
      <div className="indicator-card__header">
        <strong>{item.horizon}</strong>
        <span>{item.regime}</span>
      </div>
      <div className="indicator-card__rows">
        <span><strong>MACD</strong> {item.macdHist.toFixed(4)} / {item.macdState}</span>
        <span><strong>RSI</strong> {item.rsi14.toFixed(1)} / {item.rsiState}</span>
        <span><strong>ATR</strong> {formatPrice(item.atr14)} / {formatPercent(item.atrPct, 2)}</span>
        <span><strong>BB</strong> {formatPrice(item.bbLower)} / {formatPrice(item.bbMid)} / {formatPrice(item.bbUpper)}</span>
        <span><strong>KDJ</strong> {item.kValue.toFixed(1)} / {item.dValue.toFixed(1)} / {item.jValue.toFixed(1)}</span>
        <span><strong>State</strong> {item.bbState} / {item.kdjState}</span>
      </div>
    </article>
  );
}

export function AssetDetailPage({
  asset,
  tradePlans,
  forecasts,
  rankings,
  liveQuote
}: {
  asset: AssetRecord | null;
  tradePlans: TradePlanRecord[];
  forecasts: ForecastRecord[];
  rankings: RankingRecord[];
  liveQuote: LiveQuoteRecord | null;
}) {
  const publishedTime = formatDualTime(asset?.publishedAt);
  const rebalanceOrder = { intraday: 0, daily: 1, weekly: 2 } as const;
  const sortedTradePlans = [...tradePlans].sort((left, right) => {
    const rebalanceCompare = (rebalanceOrder[left.rebalanceFreq] ?? 99) - (rebalanceOrder[right.rebalanceFreq] ?? 99);
    if (rebalanceCompare !== 0) {
      return rebalanceCompare;
    }
    const horizonCompare = horizonSortValue(left.horizon) - horizonSortValue(right.horizon);
    if (horizonCompare !== 0) {
      return horizonCompare;
    }
    const statusOrder = { actionable: 0, stale: 1, expired: 2, filtered: 3 } as const;
    if (statusOrder[left.status] !== statusOrder[right.status]) {
      return statusOrder[left.status] - statusOrder[right.status];
    }
    if (left.selectionRank !== right.selectionRank) {
      return left.selectionRank - right.selectionRank;
    }
    return right.tradeConfidence - left.tradeConfidence;
  });
  const actionablePlans = sortedTradePlans.filter((item) => item.status === "actionable");
  const inactivePlans = sortedTradePlans.filter((item) => item.status !== "actionable");
  const intradayActionablePlans = actionablePlans.filter((item) => item.rebalanceFreq === "intraday").slice(0, 8);
  const dailyWeeklyActionablePlans = actionablePlans.filter((item) => item.rebalanceFreq !== "intraday").slice(0, 8);
  const intradayInactivePlans = inactivePlans.filter((item) => item.rebalanceFreq === "intraday").slice(0, 8);
  const dailyWeeklyInactivePlans = inactivePlans.filter((item) => item.rebalanceFreq !== "intraday").slice(0, 8);
  const sortedForecasts = [...forecasts].sort((left, right) => horizonSortValue(left.horizon) - horizonSortValue(right.horizon));
  const sortedRankings = [...rankings].sort((left, right) => {
    const rebalanceOrder = left.rebalanceFreq.localeCompare(right.rebalanceFreq);
    if (rebalanceOrder !== 0) {
      return rebalanceOrder;
    }
    return left.universe.localeCompare(right.universe);
  });
  const primaryPlan = actionablePlans[0] ?? inactivePlans[0] ?? null;
  const primaryPlanExtended = primaryPlan as (TradePlanRecord & {
    forecastValidity?: string;
    forecastConflictReason?: string | null;
    forecastAdjusted?: boolean;
    priceBasis?: string;
    executionBasis?: string;
  }) | null;
  const liveUpdated = formatDualTime(liveQuote?.updatedAt ?? null);
  const snapshotDrift =
    primaryPlan && liveQuote?.lastPrice && primaryPlan.snapshotPrice
      ? (liveQuote.lastPrice - primaryPlan.snapshotPrice) / primaryPlan.snapshotPrice
      : null;
  return (
    <Panel title="Asset Detail" eyebrow="Prediction + risk context">
      {asset ? (
        <div className="value-grid value-grid--details">
          <ValueBlock label="Live Price" primary={liveQuote ? formatPrice(liveQuote.lastPrice) : "Live quote unavailable"} secondary={liveQuote ? `${liveQuote.isStale ? "stale" : "live"} / ${liveUpdated.primary}` : "Crypto realtime overlay only"} title={liveUpdated.title} className="value-block--quote" tone={liveQuote && !liveQuote.isStale ? "accent" : "muted"} />
          <ValueBlock label="Snapshot Price" primary={primaryPlan ? formatPrice(primaryPlan.snapshotPrice) : "N/A"} secondary={primaryPlan ? `Published ${publishedTime.primary}` : "No trade plan selected"} className="value-block--quote" />
          <ValueBlock label="Drift vs Snapshot" primary={snapshotDrift === null ? "N/A" : formatSignedPercent(snapshotDrift, 2)} secondary={primaryPlan && liveQuote ? `Source ${liveQuote.source}` : "Waiting for live quote"} tone={snapshotDrift !== null && snapshotDrift < 0 ? "negative" : snapshotDrift !== null && snapshotDrift > 0 ? "positive" : "neutral"} className="value-block--quote" />
          <ValueBlock label="Symbol" primary={asset.symbol} secondary={asset.name} className="value-block--meta" />
          <ValueBlock label="Memberships" primary={asset.memberships.map(formatUniverseName).join(", ")} secondary={asset.memberships.join(", ")} title={asset.memberships.join(", ")} className="value-block--meta" />
          <ValueBlock label="Hedge Proxy" primary={asset.hedgeProxy ?? "None"} />
          <ValueBlock label="Primary Venue" primary={asset.primaryVenue} />
          <ValueBlock label="Tradable Symbol" primary={asset.tradableSymbol ?? "Research only"} />
          <ValueBlock label="Quote Asset" primary={asset.quoteAsset ?? "N/A"} />
          <ValueBlock label="Perpetual Proxy" primary={asset.hasPerpetualProxy ? "Available" : "Unavailable"} />
          <ValueBlock label="Timezone" primary={asset.timezone} />
          <ValueBlock label="Risk Bucket" primary={asset.riskBucket} />
          <ValueBlock label="Sort Rank" primary={`#${asset.sortRank}`} secondary={`${asset.sortMetricLabel} / ${formatSortMetric(asset.sortMetric, asset.sortMetricLabel)}`} />
          <ValueBlock label="History Coverage Start" primary={formatDateOnly(asset.historyCoverageStart)} secondary={asset.historyCoverageStart} title={asset.historyCoverageStart} />
          <ValueBlock label="Published At" primary={publishedTime.primary} secondary={publishedTime.secondary ?? undefined} title={publishedTime.title} />
          <ValueBlock label="Snapshot Version" primary={asset.dataSnapshotVersion} />
          <ValueBlock label="Asset Stale" primary={String(asset.stale)} />
          {primaryPlanExtended ? <ValueBlock label="Price Basis" primary={primaryPlanExtended.priceBasis ?? "asset_spot"} secondary={`Execution ${primaryPlanExtended.executionBasis ?? "native_market"}`} /> : null}
        </div>
      ) : (
        <div className="empty-state">Publish sample data to inspect an asset.</div>
      )}
      {asset ? (
        <div className="jobs-list">
          {intradayActionablePlans.length ? <span className="detail-label detail-label--terminal">Intraday Plans</span> : null}
          {intradayActionablePlans.map((item) => <PlanTicket item={item} key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}-${item.horizon}-${item.side}`} />)}
          {dailyWeeklyActionablePlans.length ? <span className="detail-label detail-label--terminal">Daily / Weekly Plans</span> : null}
          {dailyWeeklyActionablePlans.map((item) => <PlanTicket item={item} key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}-${item.horizon}-${item.side}`} />)}
          {intradayInactivePlans.length ? <span className="detail-label detail-label--terminal">Inactive Intraday Plans</span> : null}
          {intradayInactivePlans.map((item) => <PlanTicket item={item} muted key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}-${item.horizon}-${item.side}`} />)}
          {dailyWeeklyInactivePlans.length ? <span className="detail-label detail-label--terminal">Inactive Daily / Weekly Plans</span> : null}
          {dailyWeeklyInactivePlans.map((item) => <PlanTicket item={item} muted key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}-${item.horizon}-${item.side}`} />)}
          {asset.market === "index" && primaryPlanExtended ? <span className="detail-label">Index Model Diagnostics</span> : null}
          {asset.market === "index" && primaryPlanExtended ? (
            <div className="value-grid value-grid--status">
              <ValueBlock label="Forecast Validity" primary={primaryPlanExtended.forecastValidity ?? "valid"} secondary={primaryPlanExtended.forecastAdjusted ? "Auto-corrected before trade-plan generation" : "Native forecast geometry"} />
              <ValueBlock label="Conflict Reason" primary={primaryPlanExtended.forecastConflictReason ?? "None"} secondary="Direction probability and quantile consistency" />
              <ValueBlock label="Execution Reference" primary={primaryPlanExtended.executionSymbol ?? "Unavailable"} secondary={`${primaryPlanExtended.priceBasis ?? "index_spot"} / ${primaryPlanExtended.executionBasis ?? "index_reference"}`} />
            </div>
          ) : null}
          {primaryPlan ? <span className="detail-label">Support & Resistance</span> : null}
          {primaryPlan ? (
            <div className="value-grid value-grid--status">
              <ValueBlock label="Setup Type" primary={formatSetupType(primaryPlan.setupType)} secondary={formatLevelRegime(primaryPlan.levelRegime)} />
              <ValueBlock label="Nearest Support" primary={formatPrice(primaryPlan.nearestSupport)} secondary={`${formatPercent(primaryPlan.supportDistancePct, 1)} from current plan`} />
              <ValueBlock label="Nearest Resistance" primary={formatPrice(primaryPlan.nearestResistance)} secondary={`${formatPercent(primaryPlan.resistanceDistancePct, 1)} from current plan`} />
              <ValueBlock label="Support Strength" primary={formatPercent(primaryPlan.levelStrengthSupport, 0)} secondary={primaryPlan.srUnavailable ? "S/R unavailable" : "Touch / reaction strength"} />
              <ValueBlock label="Resistance Strength" primary={formatPercent(primaryPlan.levelStrengthResistance, 0)} secondary={primaryPlan.srUnavailable ? "S/R unavailable" : "Touch / reaction strength"} />
              <ValueBlock label="Plan Sources" primary={`${primaryPlan.entrySource} / ${primaryPlan.stopSource} / ${primaryPlan.targetSource}`} secondary="entry / stop / target provenance" />
            </div>
          ) : null}
          {sortedForecasts.length ? <span className="detail-label detail-label--terminal">Technical Indicators</span> : null}
          <div className="indicator-grid">
            {sortedForecasts.map((item) => (
              <TechnicalIndicatorCard item={item} key={`${item.symbol}-${item.universe}-${item.horizon}-technicals`} />
            ))}
          </div>
          {(actionablePlans.length || inactivePlans.length) && <span className="detail-label">Forecasts</span>}
          {sortedForecasts.map((item) => (
            <article className="job-row" key={`${item.symbol}-${item.universe}-${item.horizon}`}>
              <strong>{item.horizon} / {formatModelVersion(item.modelVersion).primary}</strong>
              <span>{formatPercent(item.expectedReturn, 2)} expected / {(item.pUp * 100).toFixed(0)}% up probability</span>
              <span>{formatPercent(item.q10, 2)} / {formatPercent(item.q50, 2)} / {formatPercent(item.q90, 2)}</span>
              <span>{item.regime} / {((item as ForecastRecord & { forecastValidity?: string }).forecastValidity ?? "valid")}{((item as ForecastRecord & { forecastAdjusted?: boolean }).forecastAdjusted) ? " / adjusted" : ""}{((item as ForecastRecord & { forecastConflictReason?: string | null }).forecastConflictReason) ? ` / ${((item as ForecastRecord & { forecastConflictReason?: string | null }).forecastConflictReason)}` : ""}</span>
              <span>{formatSignalProvenance(item.signalFrequency, item.sourceFrequency, item.isDerivedSignal).primary} / {formatCoverageMode(item.coverageMode)} / {item.coveragePct.toFixed(1)}%</span>
            </article>
          ))}
          {sortedRankings.length ? <span className="detail-label">Ranking Context</span> : null}
          {sortedRankings.map((item) => (
            <article className="job-row" key={`${item.symbol}-${item.universe}-${item.strategyMode}-${item.rebalanceFreq}`}>
              <strong>{formatUniverseName(item.universe)} / {formatStrategyMode(item.strategyMode)} / {formatRebalance(item.rebalanceFreq)}</strong>
              <span>score {item.score.toFixed(3)} / target {formatPercent(item.targetWeight, 2)}</span>
              <span>{formatModelVersion(item.modelVersion).primary} / {formatSignalProvenance(item.signalFrequency, item.sourceFrequency, item.isDerivedSignal).primary}</span>
              <span>{topBreakdownEntries(item.signalBreakdown).join(" / ")}</span>
            </article>
          ))}
        </div>
      ) : null}
    </Panel>
  );
}
