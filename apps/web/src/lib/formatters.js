const LOCAL_DATE_TIME = new Intl.DateTimeFormat(undefined, {
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
const DATE_ONLY = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit"
});
const MODEL_LABELS = {
    "baseline-signals-v1": "Baseline Signals",
    "crypto-ts-v1": "Crypto Time-Series",
    "equity-lgbm-ranker-v1": "Equity LGBM Ranker",
    "index-regime-v1": "Index Regime"
};
export function formatDualTime(value) {
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
export function formatDateOnly(value) {
    if (!value) {
        return "Pending";
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return value;
    }
    return DATE_ONLY.format(parsed);
}
export function formatPercent(value, decimals = 1) {
    return `${(value * 100).toFixed(decimals)}%`;
}
export function formatSignedPercent(value, decimals = 2) {
    const prefix = value > 0 ? "+" : "";
    return `${prefix}${(value * 100).toFixed(decimals)}%`;
}
export function formatDecimal(value, decimals = 2) {
    return value.toFixed(decimals);
}
export function formatPrice(value) {
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
export function formatRatio(value, decimals = 2) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return "N/A";
    }
    return `${value.toFixed(decimals)}x`;
}
export function formatSide(value) {
    return value === "long" ? "Long" : value === "short" ? "Short" : humanizeToken(value);
}
export function formatHorizon(value) {
    return value.toUpperCase();
}
export function formatStrategyMode(value) {
    return value === "long_only" ? "Long only" : value === "hedged" ? "Hedged" : humanizeToken(value);
}
export function formatRebalance(value) {
    return value === "daily" ? "Daily" : value === "weekly" ? "Weekly" : humanizeToken(value);
}
export function formatCoverageMode(value) {
    return value === "approx_bootstrap" ? "Approx. bootstrap" : value === "point_in_time" ? "Point in time" : humanizeToken(value);
}
export function formatModelVersion(value) {
    if (!value) {
        return { primary: "Unknown model", secondary: null, title: "" };
    }
    return {
        primary: MODEL_LABELS[value] ?? humanizeToken(value),
        secondary: MODEL_LABELS[value] ? value : null,
        title: value
    };
}
export function formatCompactPath(value) {
    if (!value) {
        return { primary: "Unavailable", secondary: null, title: "Unavailable" };
    }
    const parts = value.split(/[\\/]/).filter(Boolean);
    const primary = parts.at(-1) ?? value;
    const secondary = parts.length > 4 ? `.../${parts.slice(-4).join("/")}` : value;
    return { primary, secondary, title: value };
}
export function formatUniverseName(value) {
    const friendly = {
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
    return friendly[value] ?? humanizeToken(value);
}
export function formatMarketName(value) {
    const friendly = {
        crypto: "Crypto",
        cn_equity: "China A-Shares",
        us_equity: "US Equities",
        index: "Indices"
    };
    return friendly[value] ?? humanizeToken(value);
}
export function humanizeToken(value) {
    return value
        .replace(/[_-]+/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/\b\w/g, (char) => char.toUpperCase());
}
