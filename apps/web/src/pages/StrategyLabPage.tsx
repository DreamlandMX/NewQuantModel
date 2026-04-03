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
      <div className="toolbar">
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
        <div className="value-grid value-grid--details">
          <ValueBlock label="Strategy" primary={backtest.strategyId} secondary={`${formatStrategyMode(backtest.strategyMode)} / ${formatRebalance(backtest.rebalanceFreq)}`} />
          <ValueBlock label="CAGR" primary={formatPercent(backtest.cagr, 2)} />
          <ValueBlock label="Sharpe" primary={backtest.sharpe.toFixed(2)} />
          <ValueBlock label="Max Drawdown" primary={formatPercent(backtest.maxDrawdown, 2)} />
          <ValueBlock label="Turnover" primary={backtest.turnover.toFixed(2)} />
          <ValueBlock label="IC" primary={backtest.ic.toFixed(3)} />
          <ValueBlock label="Rank IC" primary={backtest.rankIc.toFixed(3)} />
          <ValueBlock label="Hit Rate" primary={formatPercent(backtest.hitRate, 1)} />
          <ValueBlock label="Top-Decile Spread" primary={formatPercent(backtest.topDecileSpread, 2)} />
          <ValueBlock label="Benchmark" primary={backtest.benchmark ?? "None"} />
          <ValueBlock label="Model" primary={formatModelVersion(backtest.modelVersion).primary} secondary={formatModelVersion(backtest.modelVersion).secondary ?? undefined} />
          <ValueBlock label="Signal Provenance" primary={provenance?.primary ?? "Unknown"} secondary={provenance?.secondary ?? undefined} />
          <ValueBlock label="Published At" primary={publishedTime.primary} secondary={publishedTime.secondary ?? undefined} title={publishedTime.title} />
          <ValueBlock label="Snapshot / Stale" primary={backtest.dataSnapshotVersion} secondary={`stale: ${String(backtest.stale)}`} />
        </div>
      ) : (
        <div className="empty-state">No backtest published yet.</div>
      )}
      {backtest ? (
        <div className="jobs-list">
          {backtest.costStress.map((item) => (
            <article className="job-row" key={item.label}>
              <strong>{item.label}</strong>
              <span>Sharpe {item.sharpe.toFixed(2)}</span>
              <span>CAGR {formatPercent(item.cagr, 2)}</span>
              <span>Max DD {formatPercent(item.maxDrawdown, 2)}</span>
            </article>
          ))}
        </div>
      ) : (
        <></>
      )}
    </Panel>
  );
}
