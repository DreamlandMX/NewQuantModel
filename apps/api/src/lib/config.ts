import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export interface AppConfig {
  appRoot: string;
  port: number;
  publishedDataDir: string;
}

function findRepoRoot(start: string): string | null {
  let current = path.resolve(start);
  while (true) {
    if (fs.existsSync(path.join(current, "pnpm-workspace.yaml")) || fs.existsSync(path.join(current, "storage", "published"))) {
      return current;
    }
    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function resolveDefaultAppRoot(): string {
  const cwdRoot = findRepoRoot(process.cwd());
  if (cwdRoot) {
    return cwdRoot;
  }
  const moduleDir = path.dirname(fileURLToPath(import.meta.url));
  const moduleRoot = findRepoRoot(moduleDir);
  if (moduleRoot) {
    return moduleRoot;
  }
  return process.cwd();
}

export function getConfig(): AppConfig {
  const appRoot = process.env.APP_ROOT || resolveDefaultAppRoot();
  return {
    appRoot,
    port: Number(process.env.PORT || 4000),
    publishedDataDir: process.env.PUBLISHED_DATA_DIR || path.join(appRoot, "storage", "published")
  };
}
