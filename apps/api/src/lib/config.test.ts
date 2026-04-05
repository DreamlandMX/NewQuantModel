import test from "node:test";
import assert from "node:assert/strict";
import path from "node:path";

import { getConfig } from "./config.js";

test("getConfig resolves published data dir from repo root when cwd is apps/api", () => {
  const previousCwd = process.cwd();
  const previousAppRoot = process.env.APP_ROOT;
  const previousPublished = process.env.PUBLISHED_DATA_DIR;

  process.chdir(path.resolve(previousCwd, "apps/api"));
  delete process.env.APP_ROOT;
  delete process.env.PUBLISHED_DATA_DIR;

  try {
    const config = getConfig();
    assert.equal(config.appRoot, previousCwd);
    assert.equal(config.publishedDataDir, path.join(previousCwd, "storage", "published"));
  } finally {
    process.chdir(previousCwd);
    if (previousAppRoot === undefined) {
      delete process.env.APP_ROOT;
    } else {
      process.env.APP_ROOT = previousAppRoot;
    }
    if (previousPublished === undefined) {
      delete process.env.PUBLISHED_DATA_DIR;
    } else {
      process.env.PUBLISHED_DATA_DIR = previousPublished;
    }
  }
});
