import { Panel } from "@newquantmodel/ui";

import type { DataHealthRecord } from "@newquantmodel/shared-types";

import { ChipList, ValueBlock } from "../components/ValueBlock";
import { formatCoverageMode, formatDualTime, formatMarketName, formatModelVersion, formatPercent } from "../lib/formatters";

export function HealthPage({
  generatedAt,
  counts,
  dataHealth,
  modelVersions
}: {
  generatedAt: string | null;
  counts: { universes: number; forecasts: number; rankings: number; jobs: number; dataHealth: number };
  dataHealth: DataHealthRecord[];
  modelVersions: string[];
}) {
  const generatedTime = formatDualTime(generatedAt);
  return (
    <Panel title="Data & Model Health" eyebrow="Pipeline state">
      <div className="value-grid value-grid--details">
        <ValueBlock label="Generated At" primary={generatedTime.primary} secondary={generatedTime.secondary ?? undefined} title={generatedTime.title} />
        <ValueBlock label="Universe Rows" primary={counts.universes} />
        <ValueBlock label="Forecast Rows" primary={counts.forecasts} />
        <ValueBlock label="Ranking Rows" primary={counts.rankings} />
        <ValueBlock label="Jobs Logged" primary={counts.jobs} />
        <ValueBlock label="Data Health Rows" primary={counts.dataHealth} />
        <article className="value-block">
          <span className="value-block__label">Model Versions</span>
          <ChipList items={(modelVersions.length > 0 ? modelVersions : ["baseline-signals-v1"]).map((item) => ({ label: formatModelVersion(item).primary, title: item }))} />
        </article>
        <ValueBlock label="Explainability" primary="Feature importance" secondary="Factor exposures + diagnostics" />
      </div>
      <div className="value-grid value-grid--status">
        {dataHealth.map((item) => (
          <ValueBlock
            key={`${item.market}-${item.lastRefreshAt}`}
            label={formatMarketName(item.market)}
            primary={`${formatPercent(item.coveragePct / 100, 1)} coverage / ${formatPercent(item.missingBarPct / 100, 1)} missing`}
            secondary={`${formatCoverageMode(item.membershipMode)} / ${item.historyStartDate} / stale: ${String(item.stale)}`}
            tertiary={item.notes.join(" | ")}
            title={item.dataSnapshotVersion}
          />
        ))}
      </div>
    </Panel>
  );
}
