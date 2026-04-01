import type { RankingRecord } from "@newquantmodel/shared-types";

import {
  formatCoverageMode,
  formatModelVersion,
  formatPercent,
  formatRebalance,
  formatStrategyMode,
  formatUniverseName
} from "../lib/formatters";

export function DataTable({ rows }: { rows: RankingRecord[] }) {
  return (
    <div className="table-shell">
      <table className="data-table">
        <thead>
          <tr>
            <th>Rank</th>
            <th>Symbol</th>
            <th>Universe</th>
            <th>Mode</th>
            <th>Rebalance</th>
            <th>Signal Family</th>
            <th>Score</th>
            <th>Expected Return</th>
            <th>Target Weight</th>
            <th>Coverage</th>
            <th>Model</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.symbol}-${row.universe}-${row.strategyMode}-${row.rebalanceFreq}`}>
              <td>{row.rank}</td>
              <td>
                <div className="table-cell-stack">
                  <strong>{row.symbol}</strong>
                </div>
              </td>
              <td title={row.universe}>
                <div className="table-cell-stack">
                  <strong>{formatUniverseName(row.universe)}</strong>
                  <span>{row.universe}</span>
                </div>
              </td>
              <td>{formatStrategyMode(row.strategyMode)}</td>
              <td>{formatRebalance(row.rebalanceFreq)}</td>
              <td title={row.signalFamily}>
                <div className="table-cell-stack">
                  <strong>{row.signalFamily.replace(/_/g, " ")}</strong>
                </div>
              </td>
              <td>{row.score.toFixed(3)}</td>
              <td>{formatPercent(row.expectedReturn, 2)}</td>
              <td>{formatPercent(row.targetWeight, 2)}</td>
              <td>
                <div className="table-cell-stack">
                  <strong>{formatCoverageMode(row.coverageMode)}</strong>
                  <span>{row.coveragePct.toFixed(1)}%</span>
                </div>
              </td>
              <td title={row.modelVersion}>
                <div className="table-cell-stack">
                  <strong>{formatModelVersion(row.modelVersion).primary}</strong>
                  <span>{formatModelVersion(row.modelVersion).secondary ?? row.modelVersion}</span>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
