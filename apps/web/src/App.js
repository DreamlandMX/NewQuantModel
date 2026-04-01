import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
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
export function App() {
    const [section, setSection] = useState("overview");
    const workbench = useWorkbenchState();
    const { health, universes, assets, dataHealth, forecasts, rankings, tradePlans, jobs, report } = useDashboardData();
    const runJob = useMutation({
        mutationFn: (type) => api.runJob(type)
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
        const versions = new Set();
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
    const sections = {
        overview: (_jsx(OverviewPage, { generatedAt: health.data?.summary.generatedAt ?? null, universes: universes.data?.items ?? [], dataHealth: dataHealth.data?.items ?? [], forecasts: forecasts.data?.items ?? [], reportPath: report.data?.pdfPath ?? null, modelVersions: modelVersions })),
        workbench: (_jsx(WorkbenchPage, { rows: filteredTradePlans, universes: filteredUniverses, market: workbench.market, strategyMode: workbench.strategyMode, rebalanceFreq: workbench.rebalanceFreq, tradableOnly: workbench.tradableOnly, onMarketChange: workbench.setMarket, onStrategyModeChange: workbench.setStrategyMode, onRebalanceFreqChange: workbench.setRebalanceFreq, onTradableOnlyChange: workbench.setTradableOnly })),
        asset: (_jsx(AssetDetailPage, { asset: assetQuery.data ?? null, tradePlans: (tradePlans.data?.items ?? []).filter((item) => item.symbol === focusedSymbol), forecasts: (forecasts.data?.items ?? []).filter((item) => item.symbol === focusedSymbol), rankings: (rankings.data?.items ?? []).filter((item) => item.symbol === focusedSymbol) })),
        strategy: _jsx(StrategyLabPage, { backtest: backtestQuery.data ?? null }),
        health: (_jsx(HealthPage, { generatedAt: health.data?.summary.generatedAt ?? null, counts: health.data?.summary ?? { universes: 0, forecasts: 0, rankings: 0, jobs: 0, dataHealth: 0, generatedAt: null }, dataHealth: dataHealth.data?.items ?? [], modelVersions: modelVersions })),
        jobs: _jsx(JobCenterPage, { jobs: jobs.data?.items ?? [], report: report.data ?? null, onRunJob: (type) => runJob.mutate(type) })
    };
    return (_jsxs("main", { className: "app-shell", children: [_jsxs("aside", { className: "sidebar", children: [_jsxs("div", { className: "brand-block", children: [_jsx("span", { className: "brand-block__eyebrow", children: "Research terminal" }), _jsx("h1", { children: "newquantmodel" }), _jsx("p", { children: "Batch-published forecasts, rankings, backtests, and research exports." })] }), _jsx("nav", { className: "nav-list", children: [
                            ["overview", "Overview"],
                            ["workbench", "Universe Workbench"],
                            ["asset", "Asset Detail"],
                            ["strategy", "Strategy Lab"],
                            ["health", "Data & Model Health"],
                            ["jobs", "Job Center"]
                        ].map(([key, label]) => (_jsx("button", { className: section === key ? "nav-list__item is-active" : "nav-list__item", onClick: () => setSection(key), children: label }, key))) })] }), _jsxs("section", { className: "content-shell", children: [_jsxs("header", { className: "content-header", children: [_jsxs("div", { className: "header-meta", children: [_jsx("span", { className: "detail-label", children: "Environment" }), _jsx("strong", { children: "Windows + WSL" }), _jsx("span", { children: "Local-first / English UI" })] }), _jsxs("div", { className: "header-meta", title: report.data?.pdfPath ?? undefined, children: [_jsx("span", { className: "detail-label", children: "Exports" }), _jsx("strong", { children: "Markdown, CSV, PDF weekly report" }), _jsx("span", { children: exportPath.secondary ?? exportPath.primary })] }), _jsxs("div", { className: "header-meta", title: publishedTime.title, children: [_jsx("span", { className: "detail-label", children: "Status" }), _jsx("strong", { children: publishedTime.primary }), _jsx("span", { children: publishedTime.secondary ?? "Awaiting first publish" })] })] }), sections[section]] })] }));
}
function filteredSymbol(tradePlans, rankings, forecasts, market) {
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
        crypto: (universe) => universe.includes("crypto"),
        cn_equity: (universe) => universe.includes("csi"),
        us_equity: (universe) => ["dow30", "nasdaq100", "sp500"].includes(universe),
        index: (universe) => universe.includes("index") || universe.includes("composite")
    };
    const fromRank = rankings.find((item) => marketMatches[market]?.(item.universe));
    if (fromRank) {
        return fromRank.symbol;
    }
    const fromForecast = forecasts.find((item) => item.market === market);
    return fromForecast?.symbol ?? "BTCUSDT";
}
