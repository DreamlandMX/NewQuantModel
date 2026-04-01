import path from "node:path";

export interface AppConfig {
  appRoot: string;
  port: number;
  publishedDataDir: string;
}

export function getConfig(): AppConfig {
  const appRoot = process.env.APP_ROOT || process.cwd();
  return {
    appRoot,
    port: Number(process.env.PORT || 4000),
    publishedDataDir: process.env.PUBLISHED_DATA_DIR || path.join(appRoot, "storage", "published")
  };
}
