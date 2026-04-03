import { useEffect, useMemo, useState } from "react";

import { Panel } from "@newquantmodel/ui";

import type { LiveQuoteRecord, TradePlanRecord, UniverseRecord } from "@newquantmodel/shared-types";

import { TradePlanTable } from "../components/TradePlanTable";
import { ValueBlock } from "../components/ValueBlock";
import { formatCoverageMode, formatMarketName, formatTradePlanStatus, formatUniverseName } from "../lib/formatters";

function tradePlanKey(row: TradePlanRecord) {
  return `${row.symbol}-${row.universe}-${row.rebalanceFreq}-${row.horizon}-${row.side}`;
}

export function WorkbenchPage({
  rows,
  universes,
  liveQuotes,
  market,
  rebalanceFreq,
  tradableOnly,
  statusFilter,
  onMarketChange,
  onRebalanceFreqChange,
  onTradableOnlyChange,
  onStatusFilterChange
}: {
  rows: TradePlanRecord[];
  universes: UniverseRecord[];
  liveQuotes: LiveQuoteRecord[];
  market: string;
  rebalanceFreq: string;
  tradableOnly: boolean;
  statusFilter: string;
  onMarketChange: (value: string) => void;
  onRebalanceFreqChange: (value: string) => void;
  onTradableOnlyChange: (value: boolean) => void;
  onStatusFilterChange: (value: string) => void;
}) {
  const liveConnected = liveQuotes.some((item) => item.market === market && !item.isStale);
  const [selectedTradePlanKey, setSelectedTradePlanKey] = useState<string | null>(null);
  const liveFeedLabel =
    market === "crypto"
      ? `Live quote feed: ${liveConnected ? "connected" : "stale or unavailable"}.`
      : market === "index"
        ? `Live index feed: ${liveConnected ? "connected" : "stale or unavailable"}.`
        : "";
  const selectedUniverse = universes[0] ?? null;
  const sortedRows = useMemo(() => rows, [rows]);

  useEffect(() => {
    if (sortedRows.length === 0) {
      setSelectedTradePlanKey(null);
      return;
    }

    if (!selectedTradePlanKey || !sortedRows.some((row) => tradePlanKey(row) === selectedTradePlanKey)) {
      setSelectedTradePlanKey(tradePlanKey(sortedRows[0]));
    }
  }, [selectedTradePlanKey, sortedRows]);

  return (
    <Panel title="Universe Workbench" eyebrow="Actionable trade plans">
      <div className="toolbar">
        <label>
          Market
          <select value={market} onChange={(event) => onMarketChange(event.target.value)}>
            <option value="crypto">Crypto</option>
            <option value="cn_equity">China A-shares</option>
            <option value="us_equity">US equities</option>
            <option value="index">Indices</option>
          </select>
        </label>
        <label>
          Rebalance
          <select value={rebalanceFreq} onChange={(event) => onRebalanceFreqChange(event.target.value)}>
            <option value="intraday">intraday</option>
            <option value="daily">daily</option>
            <option value="weekly">weekly</option>
          </select>
        </label>
        <label>
          Active only
          <input type="checkbox" checked={tradableOnly} onChange={(event) => onTradableOnlyChange(event.target.checked)} />
        </label>
        <label>
          Status
          <select value={statusFilter} onChange={(event) => onStatusFilterChange(event.target.value)}>
            <option value="all">All statuses</option>
            <option value="actionable">{formatTradePlanStatus("actionable")}</option>
            <option value="expired">{formatTradePlanStatus("expired")}</option>
            <option value="stale">{formatTradePlanStatus("stale")}</option>
            <option value="filtered">{formatTradePlanStatus("filtered")}</option>
          </select>
        </label>
      </div>
      {selectedUniverse ? (
        <div className="value-grid value-grid--status">
          <ValueBlock
            key={selectedUniverse.universe}
            label={formatUniverseName(selectedUniverse.universe)}
            primary={`${selectedUniverse.dataSource}`}
            secondary={`${formatMarketName(selectedUniverse.market)} / ${formatCoverageMode(selectedUniverse.coverageMode)} / ${selectedUniverse.coveragePct.toFixed(1)}%`}
            tertiary={`Refresh ${selectedUniverse.refreshSchedule} / stale: ${String(selectedUniverse.stale)}`}
            title={selectedUniverse.universe}
          />
        </div>
      ) : null}
      <div className="detail-label">
        Quick scan on the left, full diagnostics on the right. {rebalanceFreq === "intraday" ? "Intraday mode shows 30m, 1H, and 4H plans." : "Longer-horizon mode shows daily and weekly plans."} {market === "crypto" ? "Crypto rows overlay live exchange quotes on top of the published snapshot." : market === "index" ? "Index rows overlay live index points on top of the published snapshot while ETF or futures proxies remain execution references only." : ""} {liveFeedLabel}
      </div>
      <TradePlanTable rows={sortedRows} selectedTradePlanKey={selectedTradePlanKey} onSelectTradePlan={setSelectedTradePlanKey} />
    </Panel>
  );
}
