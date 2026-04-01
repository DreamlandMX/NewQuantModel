import { Panel } from "@newquantmodel/ui";

import type { TradePlanRecord, UniverseRecord } from "@newquantmodel/shared-types";

import { TradePlanTable } from "../components/TradePlanTable";
import { ValueBlock } from "../components/ValueBlock";
import { formatCoverageMode, formatMarketName, formatStrategyMode, formatUniverseName } from "../lib/formatters";

export function WorkbenchPage({
  rows,
  universes,
  market,
  strategyMode,
  rebalanceFreq,
  tradableOnly,
  onMarketChange,
  onStrategyModeChange,
  onRebalanceFreqChange,
  onTradableOnlyChange
}: {
  rows: TradePlanRecord[];
  universes: UniverseRecord[];
  market: string;
  strategyMode: string;
  rebalanceFreq: string;
  tradableOnly: boolean;
  onMarketChange: (value: string) => void;
  onStrategyModeChange: (value: string) => void;
  onRebalanceFreqChange: (value: string) => void;
  onTradableOnlyChange: (value: boolean) => void;
}) {
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
          Strategy
          <select value={strategyMode} onChange={(event) => onStrategyModeChange(event.target.value)}>
            <option value="long_only">{formatStrategyMode("long_only")}</option>
            <option value="hedged">{formatStrategyMode("hedged")}</option>
          </select>
        </label>
        <label>
          Rebalance
          <select value={rebalanceFreq} onChange={(event) => onRebalanceFreqChange(event.target.value)}>
            <option value="daily">daily</option>
            <option value="weekly">weekly</option>
          </select>
        </label>
        <label>
          Actionable only
          <input type="checkbox" checked={tradableOnly} onChange={(event) => onTradableOnlyChange(event.target.checked)} />
        </label>
      </div>
      <div className="value-grid value-grid--status">
        {universes.map((item) => (
          <ValueBlock
            key={item.universe}
            label={formatUniverseName(item.universe)}
            primary={`${item.dataSource}`}
            secondary={`${formatMarketName(item.market)} / ${formatCoverageMode(item.coverageMode)} / ${item.coveragePct.toFixed(1)}%`}
            tertiary={`Refresh ${item.refreshSchedule} / stale: ${String(item.stale)}`}
            title={item.universe}
          />
        ))}
      </div>
      <TradePlanTable rows={rows} />
    </Panel>
  );
}
