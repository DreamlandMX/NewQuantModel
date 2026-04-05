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
  const actionableCount = rows.filter((row) => row.status === "actionable").length;
  const filteredCount = rows.filter((row) => row.status === "filtered").length;
  const staleCount = rows.filter((row) => row.status === "stale").length;
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
      <div className="toolbar toolbar--terminal">
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
      <div className="workbench-summary">
        <ValueBlock label="Market" primary={formatMarketName(market)} secondary={rebalanceFreq === "intraday" ? "30m / 1H / 4H ladder" : `${rebalanceFreq} execution lane`} tone="accent" />
        <ValueBlock label="Visible Rows" primary={String(rows.length)} secondary={`${actionableCount} actionable / ${filteredCount} filtered`} tone="neutral" />
        <ValueBlock label="Status Risk" primary={staleCount > 0 ? `${staleCount} stale` : "Clean"} secondary={liveConnected ? "Live overlay connected" : "Overlay delayed"} tone={staleCount > 0 ? "negative" : "positive"} />
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
            className="value-block--meta"
          />
        </div>
      ) : null}
      <div className="detail-label detail-label--terminal">
        Left pane is the execution blotter, right pane is the selected setup ticket. {rebalanceFreq === "intraday" ? "Intraday mode prioritizes short-horizon laddering and drift checks." : "Longer-horizon mode emphasizes publish geometry, blockers, and expiry."} {market === "crypto" ? "Crypto rows show exchange-linked live drift against the published snapshot." : market === "index" ? "Index rows show live spot overlays while execution remains proxy-based." : ""} {liveFeedLabel}
      </div>
      <TradePlanTable rows={sortedRows} selectedTradePlanKey={selectedTradePlanKey} onSelectTradePlan={setSelectedTradePlanKey} />
    </Panel>
  );
}
