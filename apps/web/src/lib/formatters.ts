const LOCAL_DATE_TIME = new Intl.DateTimeFormat("en-US", {
  year: "numeric",
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZoneName: "short"
});

const UTC_DATE_TIME = new Intl.DateTimeFormat("en-US", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "UTC",
  timeZoneName: "short"
});

const DATE_ONLY = new Intl.DateTimeFormat("en-US", {
  year: "numeric",
  month: "short",
  day: "2-digit"
});

const MODEL_LABELS: Record<string, string> = {
  "baseline-signals-v1": "Baseline Signals",
  "crypto-ts-v1": "Crypto Time-Series",
  "equity-lgbm-ranker-v1": "Equity LGBM Ranker",
  "index-regime-v1": "Index Regime",
  "crypto-ts-sr-ind-v1": "Crypto TS + Indicators",
  "equity-lgbm-ranker-sr-ind-v1": "Equity LGBM + Indicators",
  "index-regime-sr-ind-v1": "Index Regime + Indicators"
};

export const HORIZON_SORT_ORDER: Record<string, number> = {
  "30M": 1,
  "1H": 2,
  "4H": 3,
  "1D": 4,
  "1W": 5,
  "5D": 6,
  "20D": 7,
  "5W": 8,
  "20W": 9
};

export function formatDualTime(value: string | null | undefined) {
  if (!value) {
    return { primary: "Pending", secondary: null, title: "Pending" };
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return { primary: value, secondary: null, title: value };
  }

  return {
    primary: LOCAL_DATE_TIME.format(parsed),
    secondary: `UTC ${UTC_DATE_TIME.format(parsed).replace(",", "")}`,
    title: value
  };
}

export function formatDateOnly(value: string | null | undefined) {
  if (!value) {
    return "Pending";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return DATE_ONLY.format(parsed);
}

export function formatPercent(value: number, decimals = 1) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return `${(value * 100).toFixed(decimals)}%`;
}

export function formatSignedPercent(value: number, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${(value * 100).toFixed(decimals)}%`;
}

export function formatDecimal(value: number, decimals = 2) {
  return value.toFixed(decimals);
}

export function formatPrice(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  const absolute = Math.abs(value);
  const decimals = absolute >= 1000 ? 2 : absolute >= 1 ? 2 : absolute >= 0.01 ? 4 : 6;
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  }).format(value);
}

export function formatRatio(value: number | null | undefined, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "N/A";
  }
  return `${value.toFixed(decimals)}x`;
}

export function formatBooleanLabel(value: boolean) {
  return value ? "Yes" : "No";
}

export function formatTradePlanStatus(value: string | null | undefined) {
  const friendly: Record<string, string> = {
    actionable: "Actionable",
    filtered: "Filtered",
    expired: "Expired",
    stale: "Stale"
  };
  if (!value) {
    return "Unknown";
  }
  return friendly[value] ?? humanizeToken(value);
}

export function formatBlockedReason(value: string | null | undefined) {
  if (!value) {
    return "Meets current entry / SL / TP / RR filter";
  }
  const friendly: Record<string, string> = {
    expired_at_next_rebalance: "Expired at next rebalance",
    stale_market_or_universe_data: "Blocked by stale market or universe data",
    snapshot_refresh_due: "Snapshot refresh due",
    confidence_below_55pct: "Confidence below 55%",
    trade_confidence_below_threshold: "Trade confidence below threshold",
    direction_not_bullish_enough: "Direction not bullish enough",
    direction_not_bearish_enough: "Direction not bearish enough",
    risk_reward_below_1_5x: "Risk reward below 1.5x",
    missing_quantile_forecast: "Missing quantile forecast",
    invalid_long_quantile_geometry: "Invalid long quantile geometry",
    invalid_short_quantile_geometry: "Invalid short quantile geometry",
    invalid_long_trade_geometry: "Long trade geometry is invalid",
    invalid_short_trade_geometry: "Short trade geometry is invalid",
    forecast_conflict: "Model conflict: direction and quantiles do not agree",
    direction_quantile_mismatch: "Direction probability and quantiles disagree",
    direction_quantile_mismatch_auto_flipped_bearish: "Forecast auto-corrected to bearish",
    direction_quantile_mismatch_auto_flipped_bullish: "Forecast auto-corrected to bullish",
    neutral_probability_aligned_bullish: "Neutral probability aligned bullish",
    neutral_probability_aligned_bearish: "Neutral probability aligned bearish",
    quantile_geometry_mixed: "Quantile geometry is mixed",
    zero_risk_distance: "Zero risk distance",
    non_tradable_or_missing_perpetual_proxy: "Missing tradable perpetual proxy",
    cn_equity_short_disabled: "China equity short is disabled",
    non_tradable_cash_equity: "Cash equity not tradable",
    missing_execution_symbol: "Missing execution symbol",
    missing_inverse_proxy: "Missing inverse proxy",
    missing_long_proxy: "Missing long proxy",
    missing_entry_reference_price: "Missing entry reference price",
    lost_conflict_resolution: "Lower-quality opposite side lost conflict resolution",
    indicator_alignment_below_threshold: "Indicator alignment below threshold",
    no_valid_sr_setup: "No valid support / resistance setup",
    strong_resistance_overhead: "Strong resistance overhead",
    strong_support_below: "Strong support below"
  };
  return value
    .split(",")
    .map((part) => friendly[part.trim()] ?? humanizeToken(part.trim()))
    .join(" | ");
}

export function formatBlockedReasonTags(value: string | null | undefined, max = 3) {
  if (!value) {
    return ["Pass"];
  }
  const short: Record<string, string> = {
    expired_at_next_rebalance: "Expired",
    stale_market_or_universe_data: "Stale data",
    snapshot_refresh_due: "Refresh due",
    confidence_below_55pct: "Low confidence",
    trade_confidence_below_threshold: "Low trade conf",
    direction_not_bullish_enough: "Weak long bias",
    direction_not_bearish_enough: "Weak short bias",
    risk_reward_below_1_5x: "Low RR",
    missing_quantile_forecast: "Missing quantiles",
    invalid_long_quantile_geometry: "Long geometry",
    invalid_short_quantile_geometry: "Short geometry",
    invalid_long_trade_geometry: "Invalid long",
    invalid_short_trade_geometry: "Invalid short",
    forecast_conflict: "Forecast conflict",
    direction_quantile_mismatch: "Dir mismatch",
    zero_risk_distance: "Zero risk",
    non_tradable_or_missing_perpetual_proxy: "No perp proxy",
    cn_equity_short_disabled: "CN short off",
    non_tradable_cash_equity: "No cash execution",
    missing_execution_symbol: "No execution symbol",
    missing_inverse_proxy: "No inverse proxy",
    missing_long_proxy: "No long proxy",
    missing_entry_reference_price: "Missing price",
    lost_conflict_resolution: "Conflict loser",
    indicator_alignment_below_threshold: "Low alignment",
    no_valid_sr_setup: "No S/R",
    strong_resistance_overhead: "Resistance overhead",
    strong_support_below: "Support below"
  };

  return value
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => short[part] ?? humanizeToken(part))
    .slice(0, max);
}

export function formatSide(value: string | null | undefined) {
  if (!value) {
    return "Unknown";
  }
  return value === "long" ? "Long" : value === "short" ? "Short" : humanizeToken(value);
}

export function formatHorizon(value: string | null | undefined) {
  return (value ?? "N/A").toUpperCase();
}

export function horizonSortValue(value: string | null | undefined) {
  return HORIZON_SORT_ORDER[(value ?? "").toUpperCase()] ?? 999;
}

export function formatStrategyMode(value: string | null | undefined) {
  if (!value) {
    return "Unknown";
  }
  return value === "long_only" ? "Long only" : value === "hedged" ? "Hedged" : humanizeToken(value);
}

export function formatRebalance(value: string | null | undefined) {
  if (!value) {
    return "Unknown";
  }
  return value === "intraday" ? "Intraday" : value === "daily" ? "Daily" : value === "weekly" ? "Weekly" : humanizeToken(value);
}

export function formatSignalFrequency(value: string | null | undefined) {
  return formatRebalance(value);
}

export function formatSignalProvenance(
  signalFrequency: string | null | undefined,
  sourceFrequency: string | null | undefined,
  isDerivedSignal: boolean,
  horizon?: string | null | undefined
) {
  const normalizedHorizon = (horizon ?? "").toUpperCase();
  const isIntradayExecution = normalizedHorizon === "30M" || normalizedHorizon === "1H" || normalizedHorizon === "4H";
  const signalLabel = formatSignalFrequency(signalFrequency);
  const sourceLabel = formatSignalFrequency(sourceFrequency);

  if (isIntradayExecution && signalLabel === "Daily") {
    return {
      primary: "Daily-derived intraday execution",
      secondary: `${formatHorizon(horizon)} plan from ${sourceLabel.toLowerCase()} signal`
    };
  }
  if (isDerivedSignal) {
    return {
      primary: `${signalLabel} derived`,
      secondary: `from ${sourceLabel.toLowerCase()}`
    };
  }
  return {
    primary: `${signalLabel} native`,
    secondary: `source ${sourceLabel.toLowerCase()}`
  };
}

export function formatCoverageMode(value: string | null | undefined) {
  if (!value) {
    return "Unknown";
  }
  return value === "approx_bootstrap" ? "Approx. bootstrap" : value === "point_in_time" ? "Point in time" : humanizeToken(value);
}

export function formatModelVersion(value: string | null | undefined) {
  if (!value) {
    return { primary: "Unknown model", secondary: null, title: "" };
  }
  return {
    primary: MODEL_LABELS[value] ?? humanizeToken(value),
    secondary: MODEL_LABELS[value] ? value : null,
    title: value
  };
}

export function formatCompactPath(value: string | null | undefined) {
  if (!value) {
    return { primary: "Unavailable", secondary: null, title: "Unavailable" };
  }

  const parts = value.split(/[\\/]/).filter(Boolean);
  const primary = parts.at(-1) ?? value;
  const secondary = parts.length > 4 ? `.../${parts.slice(-4).join("/")}` : value;
  return { primary, secondary, title: value };
}

export function formatRelativeTime(value: string | null | undefined, reference = new Date()) {
  if (!value) {
    return "Pending";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  const diffMs = parsed.getTime() - reference.getTime();
  const past = diffMs < 0;
  const absoluteMinutes = Math.round(Math.abs(diffMs) / 60000);
  const days = Math.floor(absoluteMinutes / 1440);
  const hours = Math.floor((absoluteMinutes % 1440) / 60);
  const minutes = absoluteMinutes % 60;
  const parts: string[] = [];
  if (days) parts.push(`${days}d`);
  if (hours) parts.push(`${hours}h`);
  if (minutes || parts.length === 0) parts.push(`${minutes}m`);
  return past ? `${parts.join(" ")} ago` : `in ${parts.join(" ")}`;
}

export function formatValidityState(item: {
  isExpired?: boolean;
  refreshDue?: boolean;
  expiresSoon?: boolean;
}) {
  if (item.isExpired) {
    return { label: "Expired", tone: "negative" as const };
  }
  if (item.refreshDue) {
    return { label: "Refresh due", tone: "warning" as const };
  }
  if (item.expiresSoon) {
    return { label: "Expires soon", tone: "warning" as const };
  }
  return { label: "Valid", tone: "positive" as const };
}

export function formatUniverseName(value: string | null | undefined) {
  const friendly: Record<string, string> = {
    crypto_top50_spot: "Crypto Top 50 Spot",
    csi300: "CSI 300",
    sse_composite: "SSE Composite",
    dow30: "Dow 30",
    dow_index: "Dow Index",
    nasdaq100: "Nasdaq 100",
    nasdaq100_index: "Nasdaq 100 Index",
    sp500: "S&P 500",
    sp500_index: "S&P 500 Index"
  };
  if (!value) {
    return "Unknown universe";
  }
  return friendly[value] ?? humanizeToken(value);
}

export function formatMarketName(value: string | null | undefined) {
  const friendly: Record<string, string> = {
    crypto: "Crypto",
    cn_equity: "China A-Shares",
    us_equity: "US Equities",
    index: "Indices"
  };
  if (!value) {
    return "Unknown market";
  }
  return friendly[value] ?? humanizeToken(value);
}

export function humanizeToken(value: string | null | undefined) {
  if (!value) {
    return "Unavailable";
  }
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

export function formatIndicatorSummary(value: string | null | undefined) {
  if (!value) {
    return "Indicator summary unavailable";
  }
  return value;
}

export function formatSortMetric(value: number | null | undefined, label: string | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return label ?? "Sort metric unavailable";
  }
  if (label === "Market cap rank") {
    const absolute = Math.abs(value);
    if (absolute >= 1_000_000_000_000) {
      return `${(value / 1_000_000_000_000).toFixed(2)}T market cap`;
    }
    if (absolute >= 1_000_000_000) {
      return `${(value / 1_000_000_000).toFixed(2)}B market cap`;
    }
    if (absolute >= 1_000_000) {
      return `${(value / 1_000_000).toFixed(2)}M market cap`;
    }
  }
  if (label === "24h turnover rank" || label === "1d turnover rank") {
    const absolute = Math.abs(value);
    if (absolute >= 1_000_000_000_000) {
      return `${(value / 1_000_000_000_000).toFixed(2)}T turnover`;
    }
    if (absolute >= 1_000_000_000) {
      return `${(value / 1_000_000_000).toFixed(2)}B turnover`;
    }
    if (absolute >= 1_000_000) {
      return `${(value / 1_000_000).toFixed(2)}M turnover`;
    }
    if (absolute >= 1_000) {
      return `${(value / 1_000).toFixed(2)}K turnover`;
    }
    return `${value.toFixed(2)} turnover`;
  }
  return label ?? "Sort metric unavailable";
}

export function formatSetupType(value: string | null | undefined) {
  const friendly: Record<string, string> = {
    breakout_long: "Breakout Long",
    bounce_long: "Bounce Long",
    breakout_short: "Breakout Short",
    bounce_short: "Bounce Short",
    none: "No valid S/R setup"
  };
  if (!value) {
    return friendly.none;
  }
  return friendly[value] ?? humanizeToken(value);
}

export function formatLevelRegime(value: string | null | undefined) {
  if (!value) {
    return "Unavailable";
  }
  return humanizeToken(value);
}
