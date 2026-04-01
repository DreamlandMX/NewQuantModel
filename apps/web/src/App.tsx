import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import { api } from "./lib/api";
import { formatCompactPath, formatDualTime } from "./lib/formatters";
import { useDashboardData, useWorkbenchState } from "./lib/hooks";
import { AssetDetailPage } from "./pages/AssetDetailPage";
import { HealthPage } from "./pages/HealthPage";
import { JobCenterPage } from "./pages/JobCenterPage";
import { OverviewPage } from "./pages/OverviewPage";
import { StrategyLabPage } from "./pages/StrategyLabPage";
import { WorkbenchPage } from "./pages/WorkbenchPage";

type Section = "overview" | "workbench" | "asset" | "strategy" | "health" | "jobs";

export function App() {
  const [section, setSection] = useState<Section>("overview");
  const workbench = useWorkbenchState();
  const { health, universes, assets, dataHealth, forecasts, rankings, tradePlans, jobs, report } = useDashboardData();
  const runJob = useMutation({
    mutationFn: (type: string) => api.runJob(type)
  });

  const focusedSymbol = filteredSymbol(tradePlans.data?.items ?? [], rankings.data?.items ?? [], forecasts.data?.items ?? [], workbench.market);
  const assetQuery = useQuery({
    queryKey: ["asset", focusedSymbol],
    queryFn: () => api.asset(focusedSymbol),
    retry: false
  });

  const strategyId = `${workbench.market}-${workbench.strategyMode}-${workbench.rebalanceFreq}`;
  const backtestQuery = useQuery({
    queryKey: ["backtest", strategyId],
    queryFn: () => api.backtest(strategyId),
    retry: false
  });

  const filteredTradePlans = useMemo(() => {
    const all = tradePlans.data?.items ?? [];
    return all
      .filter((row) => row.market === workbench.market)
      .filter((row) => row.strategyMode === workbench.strategyMode && row.rebalanceFreq === workbench.rebalanceFreq)
      .filter((row) => (workbench.tradableOnly ? row.actionable : true))
      .sort((left, right) => {
        if (Number(right.actionable) !== Number(left.actionable)) {
          return Number(right.actionable) - Number(left.actionable);
        }
        if (right.riskRewardRatio !== left.riskRewardRatio) {
          return right.riskRewardRatio - left.riskRewardRatio;
        }
        if (right.confidence !== left.confidence) {
          return right.confidence - left.confidence;
        }
        return right.expectedReturn - left.expectedReturn;
      })
      .slice(0, 40);
  }, [tradePlans.data?.items, workbench.market, workbench.rebalanceFreq, workbench.strategyMode, workbench.tradableOnly]);

  const filteredUniverses = useMemo(() => {
    return (universes.data?.items ?? []).filter((item) => (workbench.market === "index" ? item.market === "index" : item.market === workbench.market));
  }, [universes.data?.items, workbench.market]);

  const modelVersions = useMemo(() => {
    const versions = new Set<string>();
    for (const item of forecasts.data?.items ?? []) {
      versions.add(item.modelVersion);
    }
    for (const item of rankings.data?.items ?? []) {
      versions.add(item.modelVersion);
    }
    return Array.from(versions).sort();
  }, [forecasts.data?.items, rankings.data?.items]);

  const publishedTime = formatDualTime(health.data?.summary.generatedAt ?? null);
  const exportPath = formatCompactPath(report.data?.pdfPath ?? null);

  const sections: Record<Section, JSX.Element> = {
    overview: (
      <OverviewPage
        generatedAt={health.data?.summary.generatedAt ?? null}
        universes={universes.data?.items ?? []}
        dataHealth={dataHealth.data?.items ?? []}
        forecasts={forecasts.data?.items ?? []}
        reportPath={report.data?.pdfPath ?? null}
        modelVersions={modelVersions}
      />
    ),
    workbench: (
      <WorkbenchPage
        rows={filteredTradePlans}
        universes={filteredUniverses}
        market={workbench.market}
        strategyMode={workbench.strategyMode}
        rebalanceFreq={workbench.rebalanceFreq}
        tradableOnly={workbench.tradableOnly}
        onMarketChange={workbench.setMarket}
        onStrategyModeChange={workbench.setStrategyMode}
        onRebalanceFreqChange={workbench.setRebalanceFreq}
        onTradableOnlyChange={workbench.setTradableOnly}
      />
    ),
    asset: (
      <AssetDetailPage
        asset={assetQuery.data ?? null}
        tradePlans={(tradePlans.data?.items ?? []).filter((item) => item.symbol === focusedSymbol)}
        forecasts={(forecasts.data?.items ?? []).filter((item) => item.symbol === focusedSymbol)}
        rankings={(rankings.data?.items ?? []).filter((item) => item.symbol === focusedSymbol)}
      />
    ),
    strategy: <StrategyLabPage backtest={backtestQuery.data ?? null} />,
    health: (
      <HealthPage
        generatedAt={health.data?.summary.generatedAt ?? null}
        counts={health.data?.summary ?? { universes: 0, forecasts: 0, rankings: 0, jobs: 0, dataHealth: 0, generatedAt: null }}
        dataHealth={dataHealth.data?.items ?? []}
        modelVersions={modelVersions}
      />
    ),
    jobs: <JobCenterPage jobs={jobs.data?.items ?? []} report={report.data ?? null} onRunJob={(type) => runJob.mutate(type)} />
  };

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand-block">
          <span className="brand-block__eyebrow">Research terminal</span>
          <h1>newquantmodel</h1>
          <p>Batch-published forecasts, rankings, backtests, and research exports.</p>
        </div>
        <nav className="nav-list">
          {([
            ["overview", "Overview"],
            ["workbench", "Universe Workbench"],
            ["asset", "Asset Detail"],
            ["strategy", "Strategy Lab"],
            ["health", "Data & Model Health"],
            ["jobs", "Job Center"]
          ] as const).map(([key, label]) => (
            <button key={key} className={section === key ? "nav-list__item is-active" : "nav-list__item"} onClick={() => setSection(key)}>
              {label}
            </button>
          ))}
        </nav>
      </aside>
      <section className="content-shell">
        <header className="content-header">
          <div className="header-meta">
            <span className="detail-label">Environment</span>
            <strong>Windows + WSL</strong>
            <span>Local-first / English UI</span>
          </div>
          <div className="header-meta" title={report.data?.pdfPath ?? undefined}>
            <span className="detail-label">Exports</span>
            <strong>Markdown, CSV, PDF weekly report</strong>
            <span>{exportPath.secondary ?? exportPath.primary}</span>
          </div>
          <div className="header-meta" title={publishedTime.title}>
            <span className="detail-label">Status</span>
            <strong>{publishedTime.primary}</strong>
            <span>{publishedTime.secondary ?? "Awaiting first publish"}</span>
          </div>
        </header>
        {sections[section]}
      </section>
    </main>
  );
}

function filteredSymbol(
  tradePlans: { symbol: string; market: string; actionable: boolean; confidence: number; riskRewardRatio: number }[],
  rankings: { symbol: string; universe: string }[],
  forecasts: { symbol: string; market: string }[],
  market: string
) {
  const fromTradePlans = tradePlans
    .filter((item) => item.market === market)
    .sort((left, right) => {
      if (Number(right.actionable) !== Number(left.actionable)) {
        return Number(right.actionable) - Number(left.actionable);
      }
      if (right.riskRewardRatio !== left.riskRewardRatio) {
        return right.riskRewardRatio - left.riskRewardRatio;
      }
      return right.confidence - left.confidence;
    })[0];
  if (fromTradePlans) {
    return fromTradePlans.symbol;
  }

  const marketMatches = {
    crypto: (universe: string) => universe.includes("crypto"),
    cn_equity: (universe: string) => universe.includes("csi"),
    us_equity: (universe: string) => ["dow30", "nasdaq100", "sp500"].includes(universe),
    index: (universe: string) => universe.includes("index") || universe.includes("composite")
  } as const;

  const fromRank = rankings.find((item) => marketMatches[market as keyof typeof marketMatches]?.(item.universe));
  if (fromRank) {
    return fromRank.symbol;
  }
  const fromForecast = forecasts.find((item) => item.market === market);
  return fromForecast?.symbol ?? "BTCUSDT";
}
