import { spawn } from "node:child_process";
import path from "node:path";
import crypto from "node:crypto";

import type { JobRecord, JobType } from "@newquantmodel/shared-types";

import { PublishedStore } from "./published-store.js";

export class JobRunner {
  constructor(
    private readonly appRoot: string,
    private readonly store: PublishedStore
  ) {}

  async enqueue(jobType: JobType): Promise<JobRecord> {
    const now = new Date().toISOString();
    const id = crypto.randomUUID();
    const record: JobRecord = {
      id,
      type: jobType,
      status: "queued",
      requestedAt: now,
      updatedAt: now,
      message: `Queued ${jobType} job`,
      outputPath: null,
      currentStage: "queued",
      stages: [
        {
          name: "queued",
          status: "queued",
          updatedAt: now,
          message: `Queued ${jobType} job`,
          outputPath: null
        }
      ],
      lastError: null
    };

    await this.store.upsertJob(record);
    this.runInBackground(record);
    return record;
  }

  private runInBackground(record: JobRecord) {
    const root = this.appRoot;
    const child = spawn(
      "python3",
      ["-m", "newquantmodel.cli.main", "run-job", "--root", root, "--job-id", record.id, "--job-type", record.type],
      {
        cwd: path.join(root, "apps", "research", "src"),
        env: { ...process.env, PYTHONPATH: [path.join(root, "apps", "research", "src"), path.join(root, "packages", "shared-types", "python")].join(":") },
        stdio: "ignore",
        detached: true
      }
    );

    child.on("error", async (error) => {
      await this.store.upsertJob({
        ...record,
        status: "failed",
        updatedAt: new Date().toISOString(),
        message: `Failed to start ${record.type}: ${error.message}`,
        currentStage: "startup",
        lastError: error.message,
        stages: [
          ...record.stages,
          {
            name: "startup",
            status: "failed",
            updatedAt: new Date().toISOString(),
            message: `Failed to start ${record.type}: ${error.message}`,
            outputPath: null
          }
        ]
      });
    });

    child.unref();
  }
}
