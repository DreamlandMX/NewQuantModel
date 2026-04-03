import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import { api } from "./lib/api";
import { formatCompactPath, formatDualTime, horizonSortValue } from "./lib/formatters";
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
  const [strategyMode, setStrategyMode] = useState<"long_only" | "hedged">("long_only");
  const workbench = useWorkbenchState();
  const { health, universes, assets, dataHealth, forecasts, rankings, tradePlans, liveQuotes, jobs, report } = useDashboardData();
  const runJob = useMutation({
    mutationFn: (type: string) => api.runJob(type)
  });

  const focusedSymbol = filteredSymbol(tradePlans.data?.items ?? [], rankings.data?.items ?? [], forecasts.data?.items ?? [], workbench.market);
  const assetQuery = useQuery({
    queryKey: ["asset", focusedSymbol],
    queryFn: () => api.asset(focusedSymbol),
    retry: false
  });

  const strategyRebalanceFreq = workbench.rebalanceFreq === "intraday" ? "daily" : workbench.rebalanceFreq;
  const strategyId = `${workbench.market}-${strategyMode}-${strategyRebalanceFreq}`;
  const backtestQuery = useQuery({
    queryKey: ["backtest", strategyId],
    queryFn: () => api.backtest(strategyId),
    retry: false
  });

  const filteredTradePlans = useMemo(() => {
    const all = tradePlans.data?.items ?? [];
    return all
      .filter((row) => row.market === workbench.market)
      .filter((row) => row.rebalanceFreq === workbench.rebalanceFreq)
      .filter((row) => (workbench.tradableOnly ? row.status === "actionable" : true))
      .filter((row) => (workbench.statusFilter === "all" ? true : row.status === workbench.statusFilter))
      .sort((left, right) => {
        const leftSortRank = Number.isFinite(left.sortRank) ? left.sortRank : 999999;
        const rightSortRank = Number.isFinite(right.sortRank) ? right.sortRank : 999999;
        if (leftSortRank !== rightSortRank) {
          return leftSortRank - rightSortRank;
        }
        const symbolCompare = left.symbol.localeCompare(right.symbol);
        if (symbolCompare !== 0) {
          return symbolCompare;
        }
        const leftHorizon = horizonSortValue(left.horizon);
        const rightHorizon = horizonSortValue(right.horizon);
        if (leftHorizon !== rightHorizon) {
          return leftHorizon - rightHorizon;
        }
        const statusOrder = { actionable: 0, stale: 1, expired: 2, filtered: 3 } as const;
        if (statusOrder[left.status] !== statusOrder[right.status]) {
          return statusOrder[left.status] - statusOrder[right.status];
        }
        if (left.selectionRank !== right.selectionRank) {
          return left.selectionRank - right.selectionRank;
        }
        if (right.tradeConfidence !== left.tradeConfidence) {
          return right.tradeConfidence - left.tradeConfidence;
        }
        if (right.directionProbability !== left.directionProbability) {
          return right.directionProbability - left.directionProbability;
        }
        return Math.abs(right.expectedReturn) - Math.abs(left.expectedReturn);
      })
      .slice(0, 240);
  }, [tradePlans.data?.items, workbench.market, workbench.rebalanceFreq, workbench.tradableOnly, workbench.statusFilter]);

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
        tradePlans={tradePlans.data?.items ?? []}
        liveQuotes={liveQuotes.data?.items ?? []}
        reportPath={report.data?.pdfPath ?? null}
        modelVersions={modelVersions}
        scheduler={health.data?.summary.scheduler ?? null}
      />
    ),
    workbench: (
      <WorkbenchPage
        rows={filteredTradePlans}
        universes={filteredUniverses}
        liveQuotes={liveQuotes.data?.items ?? []}
        market={workbench.market}
        rebalanceFreq={workbench.rebalanceFreq}
        tradableOnly={workbench.tradableOnly}
        statusFilter={workbench.statusFilter}
        onMarketChange={workbench.setMarket}
        onRebalanceFreqChange={workbench.setRebalanceFreq}
        onTradableOnlyChange={workbench.setTradableOnly}
        onStatusFilterChange={workbench.setStatusFilter}
      />
    ),
    asset: (
      <AssetDetailPage
        asset={assetQuery.data ?? null}
        tradePlans={(tradePlans.data?.items ?? []).filter((item) => item.symbol === focusedSymbol)}
        forecasts={(forecasts.data?.items ?? []).filter((item) => item.symbol === focusedSymbol)}
        rankings={(rankings.data?.items ?? []).filter((item) => item.symbol === focusedSymbol)}
        liveQuote={(liveQuotes.data?.items ?? []).find((item) => item.symbol === (assetQuery.data?.tradableSymbol ?? focusedSymbol)) ?? (liveQuotes.data?.items ?? []).find((item) => item.symbol === focusedSymbol) ?? null}
      />
    ),
    strategy: (
      <StrategyLabPage
        backtest={backtestQuery.data ?? null}
        strategyMode={strategyMode}
        rebalanceFreq={strategyRebalanceFreq as "daily" | "weekly"}
        onStrategyModeChange={setStrategyMode}
        onRebalanceFreqChange={(value) => workbench.setRebalanceFreq(value)}
      />
    ),
    health: (
      <HealthPage
        generatedAt={health.data?.summary.generatedAt ?? null}
        counts={{
          universes: health.data?.summary.universes ?? 0,
          forecasts: health.data?.summary.forecasts ?? 0,
          rankings: health.data?.summary.rankings ?? 0,
          jobs: health.data?.summary.jobs ?? 0,
          dataHealth: health.data?.summary.dataHealth ?? 0
        }}
        dataHealth={dataHealth.data?.items ?? []}
        modelVersions={modelVersions}
        scheduler={health.data?.summary.scheduler ?? null}
      />
    ),
    jobs: (
      <JobCenterPage
        jobs={jobs.data?.items ?? []}
        report={report.data ?? null}
        scheduler={health.data?.summary.scheduler ?? null}
        dataHealth={dataHealth.data?.items ?? []}
        onRunJob={(type) => runJob.mutate(type)}
      />
    )
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
  tradePlans: { symbol: string; market: string; status: "actionable" | "filtered" | "expired" | "stale"; tradeConfidence: number; riskRewardRatio: number; selectionRank: number; sortRank: number }[],
  rankings: { symbol: string; universe: string }[],
  forecasts: { symbol: string; market: string }[],
  market: string
) {
  const fromTradePlans = tradePlans
    .filter((item) => item.market === market)
    .sort((left, right) => {
      const leftSortRank = Number.isFinite(left.sortRank) ? left.sortRank : 999999;
      const rightSortRank = Number.isFinite(right.sortRank) ? right.sortRank : 999999;
      if (leftSortRank !== rightSortRank) {
        return leftSortRank - rightSortRank;
      }
      const statusOrder = { actionable: 0, stale: 1, expired: 2, filtered: 3 } as const;
      if (statusOrder[left.status] !== statusOrder[right.status]) {
        return statusOrder[left.status] - statusOrder[right.status];
      }
      if (right.riskRewardRatio !== left.riskRewardRatio) {
        return right.riskRewardRatio - left.riskRewardRatio;
      }
      if (left.selectionRank !== right.selectionRank) {
        return left.selectionRank - right.selectionRank;
      }
      return right.tradeConfidence - left.tradeConfidence;
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
