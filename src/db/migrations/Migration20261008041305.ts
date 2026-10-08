import { Migration } from '@mikro-orm/migrations';

export class Migration20261008041305 extends Migration {

  override name = 'Migration20261008041305';

  override up(): void | Promise<void> {
    this.addSql(`create type "job_status" as enum ('queued', 'processing', 'succeeded', 'failed');`);
    this.addSql(`create table "jobs" ("id" uuid not null default gen_random_uuid(), "type" text not null, "payload" jsonb not null, "status" "job_status" not null default 'queued', "attempt" int not null default 0, "max_attempts" int not null, "available_at" timestamptz not null default now(), "processing_started_at" timestamptz null, "worker_id" text null, "result" jsonb null, "last_error" jsonb null, "created_at" timestamptz not null default now(), "updated_at" timestamptz not null default now(), primary key ("id"));`);

    this.addSql(`alter table "jobs" add constraint "jobs_max_attempts_check" check (max_attempts >= 1);`);
  }

  override down(): void | Promise<void> {
    this.addSql(`drop table if exists "jobs" cascade;`);

    this.addSql(`drop type "job_status";`);
  }

}
