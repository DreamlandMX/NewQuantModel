import type { TradePlanRecord } from "@newquantmodel/shared-types";

import {
  formatDualTime,
  formatHorizon,
  formatModelVersion,
  formatPercent,
  formatPrice,
  formatRatio,
  formatRebalance,
  formatSide,
  formatStrategyMode,
  formatUniverseName,
  humanizeToken
} from "../lib/formatters";

export function TradePlanTable({ rows }: { rows: TradePlanRecord[] }) {
  return (
    <div className="table-shell">
      <table className="data-table trade-plan-table">
        <thead>
          <tr>
            <th>Side</th>
            <th>Status</th>
            <th>Horizon</th>
            <th>Symbol</th>
            <th>Entry</th>
            <th>SL</th>
            <th>TP</th>
            <th>RR</th>
            <th>Expected</th>
            <th>Confidence</th>
            <th>Expiry</th>
            <th>Universe</th>
            <th>Strategy</th>
            <th>Model</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const expiry = formatDualTime(row.expiresAt);
            const model = formatModelVersion(row.modelVersion);
            return (
              <tr key={`${row.symbol}-${row.universe}-${row.strategyMode}-${row.rebalanceFreq}-${row.horizon}-${row.side}`}>
                <td>
                  <span className={`side-pill side-pill--${row.side}`}>{formatSide(row.side)}</span>
                </td>
                <td>
                  <div className="table-cell-stack">
                    <strong className={row.actionable ? "status-pill status-pill--actionable" : "status-pill status-pill--filtered"}>
                      {row.actionable ? "Actionable" : "Filtered"}
                    </strong>
                    <span>{row.rejectionReason ?? "Meets entry / SL / TP / RR filter"}</span>
                  </div>
                </td>
                <td>{formatHorizon(row.horizon)}</td>
                <td>
                  <div className="table-cell-stack">
                    <strong>{row.symbol}</strong>
                    <span>{row.executionSymbol && row.executionSymbol !== row.symbol ? `Exec ${row.executionSymbol}` : humanizeToken(row.executionMode)}</span>
                  </div>
                </td>
                <td>{formatPrice(row.entryPrice)}</td>
                <td>{formatPrice(row.stopLossPrice)}</td>
                <td>{formatPrice(row.takeProfitPrice)}</td>
                <td>{formatRatio(row.riskRewardRatio)}</td>
                <td>{formatPercent(row.expectedReturn, 2)}</td>
                <td>{formatPercent(row.confidence, 0)}</td>
                <td title={expiry.title}>
                  <div className="table-cell-stack">
                    <strong>{expiry.primary}</strong>
                    <span>{expiry.secondary ?? row.expiresAt}</span>
                  </div>
                </td>
                <td title={row.universe}>
                  <div className="table-cell-stack">
                    <strong>{formatUniverseName(row.universe)}</strong>
                    <span>{row.market.replace("_", " ")}</span>
                  </div>
                </td>
                <td>
                  <div className="table-cell-stack">
                    <strong>{formatStrategyMode(row.strategyMode)}</strong>
                    <span>{formatRebalance(row.rebalanceFreq)}</span>
                  </div>
                </td>
                <td title={model.title}>
                  <div className="table-cell-stack">
                    <strong>{model.primary}</strong>
                    <span>{model.secondary ?? row.modelVersion}</span>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
