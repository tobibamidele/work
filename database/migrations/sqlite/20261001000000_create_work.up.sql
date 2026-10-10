-- create_work (up)
CREATE TABLE "work" (
  "id" TEXT NOT NULL,
  "type" TEXT NOT NULL,
  "version" INTEGER NOT NULL,
  "queue" TEXT NOT NULL,
  "payload" TEXT NOT NULL,
  "status" TEXT NOT NULL CONSTRAINT "ck_work_status" CHECK ("status" IN ('available', 'running', 'completed', 'failed', 'cancelled')),
  "priority" INTEGER NOT NULL,
  "attempts" INTEGER NOT NULL,
  "max_attempts" INTEGER NOT NULL,
  "available_at" DATETIME NOT NULL,
  "started_at" DATETIME,
  "completed_at" DATETIME,
  "locked_at" DATETIME,
  "lease_until" DATETIME,
  "worker_id" TEXT,
  "cancel_requested_at" DATETIME,
  "last_error" TEXT,
  "last_error_at" DATETIME,
  "idempotency_key" TEXT,
  "unique_key" TEXT,
  "active_unique_key" TEXT,
  "schema_fingerprint" TEXT,
  "timeout_ms" INTEGER,
  "metadata" TEXT,
  "created_at" DATETIME NOT NULL,
  "updated_at" DATETIME NOT NULL,
  PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "work_active_unique_idx" ON "work" ("queue", "active_unique_key");

CREATE INDEX "work_claim_idx" ON "work" ("queue", "status", "available_at", "priority", "created_at");

CREATE INDEX "work_created_idx" ON "work" ("created_at");

CREATE INDEX "work_lease_idx" ON "work" ("status", "lease_until");

CREATE INDEX "work_type_created_idx" ON "work" ("type", "created_at");
