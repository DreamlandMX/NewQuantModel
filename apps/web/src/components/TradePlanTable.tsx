import { ChipList } from "./ValueBlock";

import type { TradePlanRecord } from "@newquantmodel/shared-types";

import {
  formatBlockedReason,
  formatBlockedReasonTags,
  formatDualTime,
  formatHorizon,
  formatIndicatorSummary,
  formatLevelRegime,
  formatMarketName,
  formatModelVersion,
  formatPercent,
  formatPrice,
  formatRatio,
  formatSignedPercent,
  formatSignalProvenance,
  formatSide,
  formatSortMetric,
  formatSetupType,
  formatTradePlanStatus,
  formatUniverseName,
  humanizeToken
} from "../lib/formatters";

type Props = {
  rows: TradePlanRecord[];
  selectedTradePlanKey: string | null;
  onSelectTradePlan: (key: string) => void;
};

function tradePlanKey(row: TradePlanRecord) {
  return `${row.symbol}-${row.universe}-${row.rebalanceFreq}-${row.horizon}-${row.side}`;
}

function DetailMetric({
  label,
  value,
  hint
}: {
  label: string;
  value: string;
  hint?: string | null;
}) {
  return (
    <div className="trade-detail-metric">
      <span className="trade-detail-metric__label">{label}</span>
      <strong className="trade-detail-metric__value">{value}</strong>
      {hint ? <span className="trade-detail-metric__hint">{hint}</span> : null}
    </div>
  );
}

export function TradePlanTable({ rows, selectedTradePlanKey, onSelectTradePlan }: Props) {
  if (rows.length === 0) {
    return (
      <div className="trade-workbench-empty">
        <div className="table-empty-state">
          <strong>No rows match the current filters.</strong>
          <span>Turn off Active only or widen the status filter to review symbol groups and timeframe order.</span>
        </div>
        <aside className="trade-detail-panel trade-detail-panel--empty">
          <span className="trade-detail-panel__eyebrow">Selection</span>
          <h3>Nothing selected</h3>
          <p>Adjust the filters on the left and choose a row to inspect the full plan, indicators, levels, and runtime diagnostics.</p>
        </aside>
      </div>
    );
  }

  const sortedRows = [...rows];
  const groupedRows = sortedRows.reduce<Array<{ symbol: string; items: TradePlanRecord[] }>>((groups, row) => {
    const current = groups.at(-1);
    if (current && current.symbol === row.symbol) {
      current.items.push(row);
      return groups;
    }
    groups.push({ symbol: row.symbol, items: [row] });
    return groups;
  }, []);

  const selectedRow = rows.find((row) => tradePlanKey(row) === selectedTradePlanKey) ?? rows[0];
  const selectedSignal = formatSignalProvenance(
    selectedRow.signalFrequency,
    selectedRow.sourceFrequency,
    selectedRow.isDerivedSignal,
    selectedRow.horizon
  );
  const selectedPlanExtended = selectedRow as TradePlanRecord & {
    forecastConflictReason?: string | null;
  };
  const selectedModel = formatModelVersion(selectedRow.modelVersion);
  const selectedExpiry = formatDualTime(selectedRow.expiresAt);
  const selectedUpdated = formatDualTime(selectedRow.liveUpdatedAt);
  const selectedBlockedReason = formatBlockedReason(selectedRow.blockedReason ?? selectedRow.rejectionReason);

  return (
    <div className="trade-workbench-layout">
      <section className="trade-master-pane">
        <div className="trade-master-pane__header">
          <span>Symbol</span>
          <span>H</span>
          <span>Live</span>
          <span>Entry</span>
          <span>SL</span>
          <span>TP</span>
          <span>RR</span>
          <span>Status</span>
        </div>
        <div className="trade-master-pane__body">
          {groupedRows.map((group) => {
            const lead = group.items[0];
            const activeCount = group.items.filter((item) => item.status === "actionable").length;
            return (
              <section className="trade-master-group" key={`${group.symbol}-group`}>
                <header className="trade-master-group__header">
                  <div className="trade-master-group__title">
                    <strong>{group.symbol}</strong>
                    <span>{lead.executionSymbol && lead.executionSymbol !== lead.symbol ? lead.executionSymbol : humanizeToken(lead.executionMode)}</span>
                  </div>
                  <div className="trade-master-group__meta">
                    <span>{Number.isFinite(lead.sortRank) && lead.sortRank > 0 ? `Rank #${lead.sortRank}` : "Rank unavailable"}</span>
                    <span>{lead.sortMetricLabel ?? "Sort metric unavailable"}</span>
                    <span>{lead.sortMetric === null ? "Metric unavailable" : formatSortMetric(lead.sortMetric, null)}</span>
                    <span>{activeCount} active / {group.items.length} total</span>
                  </div>
                </header>
                <div className="trade-master-group__rows">
                  {group.items.map((row) => {
                    const key = tradePlanKey(row);
                    const liveDisplay = row.livePrice === null ? "N/A" : formatPrice(row.livePrice);
                    const driftDisplay = row.priceDriftPct === null ? "N/A" : formatSignedPercent(row.priceDriftPct, 2);
                    const blockerTags = formatBlockedReasonTags(row.blockedReason ?? row.rejectionReason, 2);
                    return (
                      <button
                        key={key}
                        className={
                          key === tradePlanKey(selectedRow)
                            ? `trade-master-row trade-master-row--${row.status} is-selected`
                            : `trade-master-row trade-master-row--${row.status}`
                        }
                        onClick={() => onSelectTradePlan(key)}
                        type="button"
                      >
                        <span className="trade-master-row__symbol-cell">
                          <strong className="trade-master-row__symbol">{row.symbol}</strong>
                          <span className="trade-master-row__symbol-meta">
                            <span className={`side-pill side-pill--${row.side}`}>{formatSide(row.side)}</span>
                            <span>{humanizeToken(row.executionMode)}</span>
                            <span>{driftDisplay} drift</span>
                          </span>
                        </span>
                        <span>{formatHorizon(row.horizon)}</span>
                        <span>{liveDisplay}</span>
                        <span>{formatPrice(row.entryPrice)}</span>
                        <span>{formatPrice(row.stopLossPrice)}</span>
                        <span>{formatPrice(row.takeProfitPrice)}</span>
                        <span>{formatRatio(row.riskRewardRatio)}</span>
                        <span className="trade-master-row__status-cell">
                          <strong className={`status-pill status-pill--${row.status}`}>{formatTradePlanStatus(row.status)}</strong>
                          <ChipList items={blockerTags.map((label) => ({ label }))} />
                        </span>
                      </button>
                    );
                  })}
                </div>
              </section>
            );
          })}
        </div>
      </section>

      <aside className="trade-detail-panel">
        <div className="trade-detail-panel__top">
          <div>
            <span className="trade-detail-panel__eyebrow">Selected plan</span>
            <h3>{selectedRow.symbol}</h3>
            <p>
              {formatUniverseName(selectedRow.universe)} · {formatHorizon(selectedRow.horizon)} · {formatSide(selectedRow.side)}
            </p>
          </div>
          <div className="trade-detail-panel__status">
            <strong className={`status-pill status-pill--${selectedRow.status}`}>{formatTradePlanStatus(selectedRow.status)}</strong>
            <ChipList items={formatBlockedReasonTags(selectedRow.blockedReason ?? selectedRow.rejectionReason, 4).map((label) => ({ label }))} />
          </div>
        </div>

        <div className="trade-detail-summary">
          <DetailMetric
            label="Live"
            value={selectedRow.livePrice === null ? "Unavailable" : formatPrice(selectedRow.livePrice)}
            hint={
              selectedRow.livePrice === null && selectedRow.market === "index"
                ? "Index live quote unavailable"
                : selectedRow.quoteStale
                  ? "Quote stale"
                  : humanizeToken(selectedRow.priceSource ?? "live")
            }
          />
          <DetailMetric label="Snapshot" value={formatPrice(selectedRow.snapshotPrice)} hint="Published snapshot" />
          <DetailMetric
            label="Drift"
            value={selectedRow.priceDriftPct === null ? "N/A" : formatSignedPercent(selectedRow.priceDriftPct, 2)}
            hint="Live vs snapshot"
          />
          <DetailMetric label="Entry" value={formatPrice(selectedRow.entryPrice)} />
          <DetailMetric label="Stop loss" value={formatPrice(selectedRow.stopLossPrice)} />
          <DetailMetric label="Take profit" value={formatPrice(selectedRow.takeProfitPrice)} />
          <DetailMetric label="Risk reward" value={formatRatio(selectedRow.riskRewardRatio)} />
          <DetailMetric label="Expected" value={formatPercent(selectedRow.expectedReturn, 2)} />
        </div>

        <div className="trade-detail-section">
          <h4>Signal quality</h4>
          <div className="trade-detail-grid">
            <DetailMetric label="Setup" value={formatSetupType(selectedRow.setupType)} hint={formatLevelRegime(selectedRow.levelRegime)} />
            <DetailMetric label="Direction probability" value={formatPercent(selectedRow.directionProbability, 0)} />
            <DetailMetric label="Trade confidence" value={formatPercent(selectedRow.tradeConfidence, 0)} />
            <DetailMetric label="Indicator alignment" value={formatPercent(selectedRow.indicatorAlignmentScore, 0)} />
            <DetailMetric label="Signal provenance" value={selectedSignal.primary} hint={selectedSignal.secondary} />
            <DetailMetric
              label="Selection"
              value={selectedRow.selectionRank === 1 ? "Primary candidate" : `Rank ${selectedRow.selectionRank}`}
              hint={humanizeToken(selectedRow.selectionReason)}
            />
          </div>
        </div>

        <div className="trade-detail-section">
          <h4>Indicators & levels</h4>
          <div className="trade-detail-grid">
            <DetailMetric
              label="Indicators"
              value={formatIndicatorSummary(selectedRow.indicatorNotes)}
              hint={
                selectedRow.indicatorUnavailable
                  ? "Indicator warmup incomplete"
                  : `${humanizeToken(selectedRow.macdState)} / ${humanizeToken(selectedRow.rsiState)} / ${humanizeToken(selectedRow.kdjState)}`
              }
            />
            <DetailMetric
              label="Support / resistance"
              value={
                selectedRow.srUnavailable
                  ? "Unavailable"
                  : `S ${formatPrice(selectedRow.nearestSupport)} / R ${formatPrice(selectedRow.nearestResistance)}`
              }
              hint={
                selectedRow.srUnavailable
                  ? "No valid S/R setup"
                  : `${formatPercent(selectedRow.supportDistancePct, 1)} to S / ${formatPercent(selectedRow.resistanceDistancePct, 1)} to R`
              }
            />
            <DetailMetric
              label="Execution path"
              value={selectedRow.executionSymbol && selectedRow.executionSymbol !== selectedRow.symbol ? selectedRow.executionSymbol : humanizeToken(selectedRow.executionMode)}
              hint={`${humanizeToken(selectedRow.entrySource)} / ${humanizeToken(selectedRow.stopSource)} / ${humanizeToken(selectedRow.targetSource)}`}
            />
            <DetailMetric label="Universe" value={formatUniverseName(selectedRow.universe)} hint={formatMarketName(selectedRow.market)} />
          </div>
        </div>

        <div className="trade-detail-section">
          <h4>Runtime & model</h4>
          <div className="trade-detail-grid">
            <DetailMetric label="Expiry" value={selectedExpiry.primary} hint={selectedExpiry.secondary ?? selectedRow.expiresAt} />
            <DetailMetric label="Updated" value={selectedUpdated.primary} hint={selectedRow.quoteStale ? "Quote stale" : selectedUpdated.secondary ?? "Live"} />
            <DetailMetric label="Model" value={selectedModel.primary} hint={selectedModel.secondary ?? selectedRow.modelVersion} />
            <DetailMetric label="Conflict / block" value={selectedBlockedReason} hint={selectedPlanExtended.forecastConflictReason ?? null} />
          </div>
        </div>

        <div className="trade-detail-section">
          <h4>Blockers</h4>
          <ChipList items={formatBlockedReasonTags(selectedRow.blockedReason ?? selectedRow.rejectionReason, 6).map((label) => ({ label }))} />
        </div>

        <div className="trade-detail-section">
          <h4>Tags</h4>
          <ChipList
            items={[
              { label: formatTradePlanStatus(selectedRow.status) },
              { label: formatSetupType(selectedRow.setupType) },
              { label: formatUniverseName(selectedRow.universe) },
              { label: selectedSignal.primary },
              { label: selectedRow.quoteStale ? "Quote stale" : "Live linked" }
            ]}
          />
        </div>
      </aside>
    </div>
  );
}
