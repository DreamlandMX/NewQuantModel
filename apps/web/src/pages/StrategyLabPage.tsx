import { Panel } from "@newquantmodel/ui";

import type { BacktestSummary } from "@newquantmodel/shared-types";

import { ValueBlock } from "../components/ValueBlock";
import { formatDualTime, formatModelVersion, formatPercent, formatRebalance, formatSignalProvenance, formatStrategyMode } from "../lib/formatters";

export function StrategyLabPage({
  backtest,
  strategyMode,
  rebalanceFreq,
  onStrategyModeChange,
  onRebalanceFreqChange
}: {
  backtest: BacktestSummary | null;
  strategyMode: "long_only" | "hedged";
  rebalanceFreq: "daily" | "weekly";
  onStrategyModeChange: (value: "long_only" | "hedged") => void;
  onRebalanceFreqChange: (value: "daily" | "weekly") => void;
}) {
  const publishedTime = formatDualTime(backtest?.publishedAt);
  const provenance = backtest ? formatSignalProvenance(backtest.signalFrequency, backtest.sourceFrequency, backtest.isDerivedSignal) : null;
  return (
    <Panel title="Strategy Lab" eyebrow="Backtest snapshot">
      <div className="toolbar toolbar--terminal">
        <label>
          Strategy
          <select value={strategyMode} onChange={(event) => onStrategyModeChange(event.target.value as "long_only" | "hedged")}>
            <option value="long_only">{formatStrategyMode("long_only")}</option>
            <option value="hedged">{formatStrategyMode("hedged")}</option>
          </select>
        </label>
        <label>
          Rebalance
          <select value={rebalanceFreq} onChange={(event) => onRebalanceFreqChange(event.target.value as "daily" | "weekly")}>
            <option value="daily">{formatRebalance("daily")}</option>
            <option value="weekly">{formatRebalance("weekly")}</option>
          </select>
        </label>
      </div>
      {backtest ? (
        <>
          <div className="strategy-hero">
            <ValueBlock label="Strategy" primary={backtest.strategyId} secondary={`${formatStrategyMode(backtest.strategyMode)} / ${formatRebalance(backtest.rebalanceFreq)}`} className="strategy-hero__identity" tone="accent" />
            <ValueBlock label="CAGR" primary={formatPercent(backtest.cagr, 2)} tone={backtest.cagr >= 0 ? "positive" : "negative"} />
            <ValueBlock label="Sharpe" primary={backtest.sharpe.toFixed(2)} tone={backtest.sharpe >= 1 ? "positive" : backtest.sharpe <= 0 ? "negative" : "accent"} />
            <ValueBlock label="Max Drawdown" primary={formatPercent(backtest.maxDrawdown, 2)} tone="negative" />
            <ValueBlock label="Hit Rate" primary={formatPercent(backtest.hitRate, 1)} tone={backtest.hitRate >= 0.5 ? "positive" : "negative"} />
          </div>
          <div className="strategy-rail">
            <ValueBlock label="Turnover" primary={backtest.turnover.toFixed(2)} secondary="Implementation friction" />
            <ValueBlock label="IC" primary={backtest.ic.toFixed(3)} secondary="Cross-sectional signal quality" />
            <ValueBlock label="Rank IC" primary={backtest.rankIc.toFixed(3)} secondary="Ordinal predictive strength" />
            <ValueBlock label="Top-Decile Spread" primary={formatPercent(backtest.topDecileSpread, 2)} secondary="Long-short edge" />
            <ValueBlock label="Benchmark" primary={backtest.benchmark ?? "None"} secondary="Comparative context" className="value-block--meta" />
            <ValueBlock label="Model" primary={formatModelVersion(backtest.modelVersion).primary} secondary={formatModelVersion(backtest.modelVersion).secondary ?? undefined} className="value-block--meta" />
            <ValueBlock label="Signal Provenance" primary={provenance?.primary ?? "Unknown"} secondary={provenance?.secondary ?? undefined} className="value-block--meta" />
            <ValueBlock label="Published At" primary={publishedTime.primary} secondary={publishedTime.secondary ?? undefined} title={publishedTime.title} className="value-block--meta" />
            <ValueBlock label="Snapshot / Stale" primary={backtest.dataSnapshotVersion} secondary={`stale: ${String(backtest.stale)}`} className="value-block--meta" />
          </div>
        </>
      ) : (
        <div className="empty-state">No backtest published yet.</div>
      )}
      {backtest ? (
        <div className="scenario-grid">
          {backtest.costStress.map((item) => (
            <article className="scenario-card" key={item.label}>
              <span className="scenario-card__label">{item.label}</span>
              <strong>{item.sharpe.toFixed(2)} Sharpe</strong>
              <span>{formatPercent(item.cagr, 2)} CAGR</span>
              <span>{formatPercent(item.maxDrawdown, 2)} Max DD</span>
            </article>
          ))}
        </div>
      ) : (
        <></>
      )}
    </Panel>
  );
}
