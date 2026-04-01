const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:4000";
async function getJson(path) {
    const response = await fetch(`${API_BASE_URL}${path}`);
    if (!response.ok) {
        throw new Error(`Request failed for ${path}`);
    }
    return response.json();
}
async function postJson(path, body) {
    const response = await fetch(`${API_BASE_URL}${path}`, {
        method: "POST",
        headers: {
            "content-type": "application/json"
        },
        body: JSON.stringify(body)
    });
    if (!response.ok) {
        throw new Error(`Request failed for ${path}`);
    }
    return response.json();
}
export const api = {
    health: () => getJson("/health"),
    universes: () => getJson("/api/universes"),
    assets: () => getJson("/api/assets"),
    dataHealth: () => getJson("/api/health/data"),
    forecasts: (query = "") => getJson(`/api/forecasts${query}`),
    rankings: (query = "") => getJson(`/api/rankings${query}`),
    tradePlans: (query = "") => getJson(`/api/trade-plans${query}`),
    asset: (symbol) => getJson(`/api/assets/${encodeURIComponent(symbol)}`),
    backtest: (strategyId) => getJson(`/api/backtests/${encodeURIComponent(strategyId)}`),
    jobs: () => getJson("/api/jobs"),
    runJob: (type) => postJson("/api/jobs/run", { type }),
    latestReport: () => getJson("/api/reports/latest")
};
