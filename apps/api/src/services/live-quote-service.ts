import type { LiveQuoteRecord } from "@newquantmodel/shared-types";

type OkxTickerResponse = {
  code: string;
  data?: Array<{
    instId: string;
    last: string;
    open24h: string;
    ts: string;
  }>;
};

type OkxMarkPriceResponse = {
  code: string;
  data?: Array<{
    instId: string;
    markPx: string;
    ts: string;
  }>;
};

type YahooChartResponse = {
  chart?: {
    result?: Array<{
      meta?: {
        regularMarketPrice?: number;
        chartPreviousClose?: number;
        regularMarketTime?: number;
      };
      timestamp?: number[];
      indicators?: {
        quote?: Array<{
          close?: Array<number | null>;
        }>;
      };
    }>;
  };
};

type StoredQuote = Omit<LiveQuoteRecord, "isStale">;

const OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers?instType=SWAP";
const OKX_MARK_PRICE_URL = "https://www.okx.com/api/v5/public/mark-price?instType=SWAP";
const YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart";
const INDEX_SYMBOLS = ["^GSPC", "^NDX", "^DJI", "000001.SS", "000300.SS"];

export class CryptoLiveQuoteService {
  private readonly quotes = new Map<string, StoredQuote>();
  private timer: NodeJS.Timeout | null = null;
  private refreshing = false;
  private readonly staleMs = 20_000;

  async start() {
    await this.refresh();
    this.timer = setInterval(() => {
      void this.refresh();
    }, 5_000);
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async refresh() {
    if (this.refreshing) {
      return;
    }

    this.refreshing = true;
    try {
      await Promise.allSettled([this.refreshCryptoQuotes(), this.refreshIndexQuotes()]);
    } finally {
      this.refreshing = false;
    }
  }

  getQuotes(params?: { market?: string; symbols?: string[] }): { items: LiveQuoteRecord[] } {
    const normalizedSymbols = params?.symbols?.length
      ? new Set(params.symbols.map((item) => normalizeSymbol(item)))
      : null;

    const items = [...this.quotes.values()]
      .filter((item) => {
        if (params?.market && item.market !== params.market) {
          return false;
        }
        if (normalizedSymbols && !normalizedSymbols.has(normalizeSymbol(item.symbol))) {
          return false;
        }
        return true;
      })
      .sort((left, right) => {
        const marketCompare = left.market.localeCompare(right.market);
        if (marketCompare !== 0) {
          return marketCompare;
        }
        return left.symbol.localeCompare(right.symbol);
      })
      .map((item) => this.decorate(item));

    return { items };
  }

  getQuote(symbol: string): LiveQuoteRecord | null {
    const normalized = normalizeSymbol(symbol);
    const quote = [...this.quotes.values()].find((item) => normalizeSymbol(item.symbol) === normalized) ?? null;
    return quote ? this.decorate(quote) : null;
  }

  private async refreshCryptoQuotes() {
    const [tickers, marks] = await Promise.all([
      this.fetchJson<OkxTickerResponse>(OKX_TICKERS_URL),
      this.fetchJson<OkxMarkPriceResponse>(OKX_MARK_PRICE_URL),
    ]);
    const markMap = new Map<string, { markPrice: number | null; updatedAt: string | null }>();

    for (const item of marks.data ?? []) {
      const symbol = toWorkspaceCryptoSymbol(item.instId);
      if (!symbol) {
        continue;
      }
      markMap.set(symbol, {
        markPrice: toFiniteNumber(item.markPx),
        updatedAt: toIsoFromMillis(item.ts),
      });
    }

    for (const item of tickers.data ?? []) {
      const symbol = toWorkspaceCryptoSymbol(item.instId);
      const lastPrice = toFiniteNumber(item.last);
      if (!symbol || lastPrice === null) {
        continue;
      }
      const open24h = toFiniteNumber(item.open24h);
      const priceChangePct24h = open24h && open24h !== 0 ? (lastPrice - open24h) / open24h : null;
      const mark = markMap.get(symbol);
      const updatedAt = toIsoFromMillis(item.ts) ?? mark?.updatedAt ?? new Date().toISOString();

      this.quotes.set(symbol, {
        symbol,
        market: "crypto",
        lastPrice,
        markPrice: mark?.markPrice ?? null,
        priceChangePct24h,
        updatedAt,
        source: "okx-swap-rest",
      });
    }
  }

  private async refreshIndexQuotes() {
    const responses = await Promise.allSettled(INDEX_SYMBOLS.map((symbol) => this.fetchIndexQuote(symbol)));
    for (const result of responses) {
      if (result.status !== "fulfilled" || !result.value) {
        continue;
      }
      this.quotes.set(result.value.symbol, result.value);
    }
  }

  private async fetchIndexQuote(symbol: string): Promise<StoredQuote | null> {
    const encoded = encodeURIComponent(symbol);
    const response = await this.fetchJson<YahooChartResponse>(`${YAHOO_CHART_URL}/${encoded}?range=1d&interval=1m&includePrePost=true`);
    const payload = response.chart?.result?.[0];
    const meta = payload?.meta ?? {};
    const closes = payload?.indicators?.quote?.[0]?.close ?? [];
    const timestamps = payload?.timestamp ?? [];
    const lastClose = [...closes].reverse().find((value) => typeof value === "number" && Number.isFinite(value)) ?? null;
    const lastPrice = toFiniteNumber(meta.regularMarketPrice) ?? toFiniteNumber(lastClose);
    if (lastPrice === null) {
      return null;
    }
    const previousClose = toFiniteNumber(meta.chartPreviousClose);
    const priceChangePct24h = previousClose && previousClose !== 0 ? (lastPrice - previousClose) / previousClose : null;
    const updatedAt = toIsoFromSeconds(meta.regularMarketTime) ?? toIsoFromSeconds([...timestamps].pop()) ?? new Date().toISOString();

    return {
      symbol,
      market: "index",
      lastPrice,
      markPrice: null,
      priceChangePct24h,
      updatedAt,
      source: "yahoo-index-rest",
    };
  }

  private decorate(item: StoredQuote): LiveQuoteRecord {
    const updatedAt = new Date(item.updatedAt);
    const ageMs = Number.isNaN(updatedAt.getTime()) ? Number.POSITIVE_INFINITY : Date.now() - updatedAt.getTime();
    return {
      ...item,
      isStale: ageMs > this.staleMs,
    };
  }

  private async fetchJson<T>(url: string): Promise<T> {
    const response = await fetch(url, {
      headers: {
        "user-agent": "newquantmodel/0.1",
      },
    });
    if (!response.ok) {
      throw new Error(`Live quote request failed for ${url}: ${response.status}`);
    }
    return response.json() as Promise<T>;
  }
}

function toWorkspaceCryptoSymbol(instId: string): string | null {
  if (!instId.endsWith("-USDT-SWAP")) {
    return null;
  }
  return instId.replace(/-SWAP$/, "").replace(/-/g, "");
}

function normalizeSymbol(value: string): string {
  return value.replace(/[-_/]/g, "").toUpperCase();
}

function toFiniteNumber(value: number | string | null | undefined): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toIsoFromMillis(value: string | number | null | undefined): string | null {
  const parsed = toFiniteNumber(value);
  if (parsed === null) {
    return null;
  }
  return new Date(parsed).toISOString();
}

function toIsoFromSeconds(value: number | string | null | undefined): string | null {
  const parsed = toFiniteNumber(value);
  if (parsed === null) {
    return null;
  }
  return new Date(parsed * 1000).toISOString();
}
