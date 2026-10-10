-- create_work (up)
CREATE TABLE `work` (
  `id` CHAR(36) NOT NULL,
  `type` VARCHAR(255) NOT NULL,
  `version` INT NOT NULL,
  `queue` VARCHAR(255) NOT NULL,
  `payload` JSON NOT NULL,
  `status` ENUM('available', 'running', 'completed', 'failed', 'cancelled') NOT NULL,
  `priority` INT NOT NULL,
  `attempts` INT NOT NULL,
  `max_attempts` INT NOT NULL,
  `available_at` DATETIME(6) NOT NULL,
  `started_at` DATETIME(6) NULL,
  `completed_at` DATETIME(6) NULL,
  `locked_at` DATETIME(6) NULL,
  `lease_until` DATETIME(6) NULL,
  `worker_id` VARCHAR(255) NULL,
  `cancel_requested_at` DATETIME(6) NULL,
  `last_error` TEXT NULL,
  `last_error_at` DATETIME(6) NULL,
  `idempotency_key` VARCHAR(255) NULL,
  `unique_key` VARCHAR(255) NULL,
  `active_unique_key` VARCHAR(255) NULL,
  `schema_fingerprint` VARCHAR(64) NULL,
  `timeout_ms` BIGINT NULL,
  `metadata` JSON NULL,
  `created_at` DATETIME(6) NOT NULL,
  `updated_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE UNIQUE INDEX `work_active_unique_idx` ON `work` (`queue`, `active_unique_key`);

CREATE INDEX `work_claim_idx` ON `work` (`queue`, `status`, `available_at`, `priority`, `created_at`);

CREATE INDEX `work_created_idx` ON `work` (`created_at`);

CREATE INDEX `work_lease_idx` ON `work` (`status`, `lease_until`);

CREATE INDEX `work_type_created_idx` ON `work` (`type`, `created_at`);
