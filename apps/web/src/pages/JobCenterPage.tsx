import type { DataHealthRecord, JobRecord, ReportManifest } from "@newquantmodel/shared-types";

import { Panel } from "@newquantmodel/ui";

import { ValueBlock } from "../components/ValueBlock";
import { formatCompactPath, formatDualTime, formatMarketName, humanizeToken } from "../lib/formatters";

const JOB_TYPES = ["ingest", "feature", "train", "backtest", "publish", "report"];

export function JobCenterPage({
  jobs,
  report,
  scheduler,
  dataHealth,
  onRunJob
}: {
  jobs: JobRecord[];
  report: ReportManifest | null;
  scheduler: {
    workerStatus: "running" | "stopped";
    heartbeatAt: string | null;
    lastError: string | null;
    pollSeconds: number | null;
    markets: Array<{
      market: string;
      lastSuccessAt: string | null;
      nextScheduledAt: string | null;
      lastCompletedBucket: string | null;
      lastError: string | null;
    }>;
  } | null;
  dataHealth: DataHealthRecord[];
  onRunJob: (type: string) => void;
}) {
  const workerHeartbeat = formatDualTime(scheduler?.heartbeatAt ?? null);
  return (
    <Panel title="Job Center" eyebrow="Batch control">
      <div className="job-actions">
        {JOB_TYPES.map((jobType) => (
          <button key={jobType} onClick={() => onRunJob(jobType)}>
            Run {jobType}
          </button>
        ))}
      </div>
      <div className="jobs-list">
        {jobs.map((job) => (
          <article className="job-row" key={job.id}>
            <strong>{humanizeToken(job.type)}</strong>
            <span>{humanizeToken(job.status)}</span>
            <span>{job.currentStage ?? "No stage yet"} / {job.message}</span>
            <span title={job.outputPath ?? undefined}>{formatCompactPath(job.outputPath).secondary ?? formatCompactPath(job.outputPath).primary}</span>
            {job.lastError ? <span>Error: {job.lastError}</span> : null}
            <div className="job-stages">
              {job.stages.map((stage) => (
                <span className={`job-stage job-stage--${stage.status}`} key={`${job.id}-${stage.name}-${stage.updatedAt}`}>
                  {humanizeToken(stage.name)}: {humanizeToken(stage.status)}
                </span>
              ))}
            </div>
          </article>
        ))}
      </div>
      <div className="value-grid value-grid--status">
        <ValueBlock
          label="Worker status"
          primary={scheduler?.workerStatus === "running" ? "Auto-refresh active" : "Worker stopped"}
          secondary={workerHeartbeat.primary}
          tertiary={scheduler?.lastError ?? `Polling every ${scheduler?.pollSeconds ?? 60}s`}
        />
        {scheduler?.markets.map((item) => {
          const health = dataHealth.find((row) => row.market === item.market);
          return (
            <ValueBlock
              key={`worker-${item.market}`}
              label={`${formatMarketName(item.market)} worker`}
              primary={`Next ${formatDualTime(item.nextScheduledAt).primary}`}
              secondary={`Last success ${formatDualTime(item.lastSuccessAt).primary}`}
              tertiary={item.lastError ?? (health?.notes.join(" | ") || "No recent errors")}
            />
          );
        })}
      </div>
      {report ? (
        <div className="value-grid value-grid--details">
          <ValueBlock
            label="Latest PDF report"
            primary={formatCompactPath(report.pdfPath).primary}
            secondary={formatCompactPath(report.pdfPath).secondary ?? undefined}
            title={report.pdfPath}
          />
          <ValueBlock
            label="Generated At"
            primary={formatDualTime(report.generatedAt).primary}
            secondary={formatDualTime(report.generatedAt).secondary ?? undefined}
            title={report.generatedAt}
          />
          <ValueBlock
            label="Markdown report"
            primary={formatCompactPath(report.markdownPath).primary}
            secondary={formatCompactPath(report.markdownPath).secondary ?? undefined}
            title={report.markdownPath}
          />
        </div>
      ) : (
        <div className="report-box">
          <strong>No report manifest yet.</strong>
        </div>
      )}
    </Panel>
  );
}
