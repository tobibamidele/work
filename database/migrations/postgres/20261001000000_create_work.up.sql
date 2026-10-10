-- create_work (up)
CREATE TABLE "work" (
  "id" UUID NOT NULL,
  "type" VARCHAR(255) NOT NULL,
  "version" INTEGER NOT NULL,
  "queue" VARCHAR(255) NOT NULL,
  "payload" JSONB NOT NULL,
  "status" TEXT NOT NULL CONSTRAINT "ck_work_status" CHECK ("status" IN ('available', 'running', 'completed', 'failed', 'cancelled')),
  "priority" INTEGER NOT NULL,
  "attempts" INTEGER NOT NULL,
  "max_attempts" INTEGER NOT NULL,
  "available_at" TIMESTAMPTZ NOT NULL,
  "started_at" TIMESTAMPTZ,
  "completed_at" TIMESTAMPTZ,
  "locked_at" TIMESTAMPTZ,
  "lease_until" TIMESTAMPTZ,
  "worker_id" VARCHAR(255),
  "cancel_requested_at" TIMESTAMPTZ,
  "last_error" TEXT,
  "last_error_at" TIMESTAMPTZ,
  "idempotency_key" VARCHAR(255),
  "unique_key" VARCHAR(255),
  "active_unique_key" VARCHAR(255),
  "schema_fingerprint" VARCHAR(64),
  "timeout_ms" BIGINT,
  "metadata" JSONB,
  "created_at" TIMESTAMPTZ NOT NULL,
  "updated_at" TIMESTAMPTZ NOT NULL,
  PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "work_active_unique_idx" ON "work" ("queue", "active_unique_key");

CREATE INDEX "work_claim_idx" ON "work" ("queue", "status", "available_at", "priority", "created_at");

CREATE INDEX "work_created_idx" ON "work" ("created_at");

CREATE INDEX "work_lease_idx" ON "work" ("status", "lease_until");

CREATE INDEX "work_type_created_idx" ON "work" ("type", "created_at");
