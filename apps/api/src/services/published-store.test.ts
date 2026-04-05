import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { PublishedStore } from "./published-store.js";

async function withTempPublishedDir(run: (dir: string) => Promise<void>) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "nqm-published-"));
  try {
    await run(dir);
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

test("published store marks plans expired and keeps published geometry unchanged", async () => {
  await withTempPublishedDir(async (dir) => {
    const store = new PublishedStore(dir, null);
    await fs.writeFile(
      path.join(dir, "trade-plans.json"),
      JSON.stringify({
        items: [
          {
            symbol: "BTCUSDT",
            market: "crypto",
            universe: "crypto_top50_spot",
            horizon: "1H",
            rebalanceFreq: "intraday",
            side: "long",
            riskPct: 0.01,
            entryPrice: 100,
            stopLossPrice: 99,
            takeProfitPrice: 102,
            stale: false,
            actionable: true,
            rejectionReason: null,
            executionSymbol: "BTCUSDT",
            expiresAt: "2026-04-05T01:00:00Z",
            validFrom: "2026-04-05T00:00:00Z",
            validUntil: "2026-04-05T01:00:00Z",
            validityMode: "bar_boundary",
            nextBarAt: "2026-04-05T01:00:00Z",
            publishedAt: "2026-04-05T00:05:00Z"
          }
        ]
      }),
      "utf8"
    );

    const result = await store.getTradePlans({ actionableOnly: false });
    const row = result.items[0];

    assert.equal(row.entryPrice, 100);
    assert.equal(row.stopLossPrice, 99);
    assert.equal(row.takeProfitPrice, 102);
    assert.equal(row.status, "expired");
    assert.equal(row.isExpired, true);
    assert.equal(row.validityMode, "bar_boundary");
    assert.equal(row.validUntil, "2026-04-05T01:00:00Z");
    assert.equal(row.nextBarAt, "2026-04-05T01:00:00Z");
  });
});

test("published store marks plans expiring soon and refresh due before expiry", async () => {
  await withTempPublishedDir(async (dir) => {
    const now = new Date();
    const validFrom = new Date(now.getTime() - 55 * 60 * 1000).toISOString();
    const validUntil = new Date(now.getTime() + 5 * 60 * 1000).toISOString();
    const liveQuotes = {
      getQuote() {
        return {
          symbol: "BTCUSDT",
          market: "crypto",
          lastPrice: 104,
          markPrice: 104,
          priceChangePct24h: 0.02,
          updatedAt: "2026-04-05T00:54:00Z",
          source: "test",
          isStale: false
        };
      },
      getQuotes() {
        return { items: [] };
      }
    };
    const store = new PublishedStore(dir, liveQuotes as never);
    await fs.writeFile(
      path.join(dir, "trade-plans.json"),
      JSON.stringify({
        items: [
          {
            symbol: "BTCUSDT",
            market: "crypto",
            universe: "crypto_top50_spot",
            horizon: "1H",
            rebalanceFreq: "intraday",
            side: "long",
            riskPct: 0.02,
            entryPrice: 100,
            stopLossPrice: 98,
            takeProfitPrice: 105,
            stale: false,
            actionable: true,
            rejectionReason: null,
            executionSymbol: "BTCUSDT",
            expiresAt: validUntil,
            validFrom,
            validUntil,
            validityMode: "bar_boundary",
            nextBarAt: validUntil,
            publishedAt: validFrom
          }
        ]
      }),
      "utf8"
    );

    const result = await store.getTradePlans({ actionableOnly: false });
    const row = result.items[0];

    assert.equal(row.isExpired, false);
    assert.equal(row.expiresSoon, true);
    assert.equal(row.refreshDue, true);
    assert.ok(row.runtimeFlags.includes("price_far_from_entry"));
  });
});
