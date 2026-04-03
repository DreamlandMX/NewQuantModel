import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "./api";

export function useDashboardData() {
  const refreshMs = 10_000;
  const health = useQuery({ queryKey: ["health"], queryFn: api.health, refetchInterval: refreshMs });
  const universes = useQuery({ queryKey: ["universes"], queryFn: api.universes, refetchInterval: refreshMs });
  const assets = useQuery({ queryKey: ["assets"], queryFn: api.assets, refetchInterval: refreshMs });
  const dataHealth = useQuery({ queryKey: ["data-health"], queryFn: api.dataHealth, refetchInterval: refreshMs });
  const forecasts = useQuery({ queryKey: ["forecasts"], queryFn: () => api.forecasts(), refetchInterval: refreshMs });
  const rankings = useQuery({ queryKey: ["rankings"], queryFn: () => api.rankings(), refetchInterval: refreshMs });
  const tradePlans = useQuery({ queryKey: ["trade-plans"], queryFn: () => api.tradePlans("?actionableOnly=false"), refetchInterval: refreshMs });
  const liveQuotes = useQuery({ queryKey: ["live-quotes"], queryFn: () => api.liveQuotes(), refetchInterval: 5_000 });
  const jobs = useQuery({ queryKey: ["jobs"], queryFn: api.jobs, refetchInterval: 5_000 });
  const report = useQuery({ queryKey: ["report"], queryFn: api.latestReport, retry: false, refetchInterval: refreshMs });

  return { health, universes, assets, dataHealth, forecasts, rankings, tradePlans, liveQuotes, jobs, report };
}

export function useWorkbenchState() {
  const [market, setMarket] = useState("crypto");
  const [rebalanceFreq, setRebalanceFreq] = useState("daily");
  const [tradableOnly, setTradableOnly] = useState(false);
  const [statusFilter, setStatusFilter] = useState("all");

  return useMemo(
    () => ({
      market,
      setMarket,
      rebalanceFreq,
      setRebalanceFreq,
      tradableOnly,
      setTradableOnly,
      statusFilter,
      setStatusFilter
    }),
    [market, rebalanceFreq, tradableOnly, statusFilter]
  );
}
