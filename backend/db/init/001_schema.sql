create table if not exists environments (
  id varchar(32) primary key,
  name varchar(64) not null,
  kind varchar(16) not null,
  region varchar(64) not null,
  cluster_label varchar(64) not null,
  summary varchar(255) not null,
  broker_url varchar(255) not null,
  admin_url varchar(255) not null,
  auth_mode varchar(32) not null,
  credential_reference varchar(500),
  sync_targets text,
  tls_enabled boolean not null default false,
  status varchar(16) not null,
  sync_status varchar(32) not null default 'NOT_SYNCED',
  sync_message varchar(255),
  last_synced_at timestamp,
  last_test_status varchar(32) not null default 'NOT_TESTED',
  last_test_message varchar(255),
  last_tested_at timestamp,
  deleted_at timestamp,
  created_at timestamp not null default current_timestamp,
  updated_at timestamp not null default current_timestamp
);

alter table if exists environments add column if not exists broker_url varchar(255);
alter table if exists environments add column if not exists admin_url varchar(255);
alter table if exists environments add column if not exists kind varchar(16);
alter table if exists environments add column if not exists region varchar(64);
alter table if exists environments add column if not exists cluster_label varchar(64);
alter table if exists environments add column if not exists summary varchar(255);
alter table if exists environments add column if not exists auth_mode varchar(32);
alter table if exists environments add column if not exists credential_reference varchar(500);
alter table if exists environments add column if not exists sync_targets text;
alter table if exists environments add column if not exists tls_enabled boolean default false;
alter table if exists environments add column if not exists status varchar(16);
alter table if exists environments add column if not exists sync_status varchar(32) default 'NOT_SYNCED';
alter table if exists environments add column if not exists sync_message varchar(255);
alter table if exists environments add column if not exists last_synced_at timestamp;
alter table if exists environments add column if not exists last_test_status varchar(32) default 'NOT_TESTED';
alter table if exists environments add column if not exists last_test_message varchar(255);
alter table if exists environments add column if not exists last_tested_at timestamp;
alter table if exists environments add column if not exists deleted_at timestamp;
alter table if exists environments add column if not exists created_at timestamp default current_timestamp;
alter table if exists environments add column if not exists updated_at timestamp default current_timestamp;
alter table if exists environments alter column credential_reference type varchar(500);

update environments
set kind = coalesce(kind, id),
    region = coalesce(region, 'local'),
    cluster_label = coalesce(cluster_label, 'cluster-' || id),
    summary = coalesce(summary, 'Imported local environment configuration'),
    broker_url = coalesce(broker_url, 'pulsar://' || id || '-brokers:6650'),
    admin_url = coalesce(admin_url, 'https://' || id || '-admin.internal'),
    auth_mode = coalesce(auth_mode, 'none'),
    tls_enabled = coalesce(tls_enabled, false),
    status = coalesce(status, 'DEGRADED'),
    sync_status = coalesce(sync_status, 'NOT_SYNCED'),
    last_test_status = coalesce(last_test_status, 'NOT_TESTED'),
    created_at = coalesce(created_at, current_timestamp),
    updated_at = coalesce(updated_at, current_timestamp);

alter table if exists environments alter column kind set not null;
alter table if exists environments alter column region set not null;
alter table if exists environments alter column cluster_label set not null;
alter table if exists environments alter column summary set not null;
alter table if exists environments alter column broker_url set not null;
alter table if exists environments alter column admin_url set not null;
alter table if exists environments alter column auth_mode set not null;
alter table if exists environments alter column tls_enabled set not null;
alter table if exists environments alter column status set not null;
alter table if exists environments alter column sync_status set not null;
alter table if exists environments alter column last_test_status set not null;
alter table if exists environments alter column created_at set not null;
alter table if exists environments alter column updated_at set not null;

create table if not exists environment_snapshots (
  environment_id varchar(32) primary key references environments(id),
  health_status varchar(16) not null,
  health_message varchar(255) not null,
  pulsar_version varchar(32) not null,
  broker_url varchar(255) not null,
  admin_url varchar(255) not null,
  tenant_count integer not null default 0,
  namespace_count integer not null default 0,
  topic_count integer not null default 0,
  tenants_json text not null,
  namespaces_json text not null,
  topics_json text not null,
  created_at timestamp not null default current_timestamp,
  updated_at timestamp not null default current_timestamp
);

create table if not exists jobs (
  id varchar(64) primary key,
  type varchar(32) not null,
  environment_id varchar(32) not null references environments(id),
  status varchar(16) not null,
  parameters text not null default '{}',
  created_at timestamp not null default current_timestamp,
  updated_at timestamp not null default current_timestamp
);

create table if not exists job_events (
  id bigint generated by default as identity primary key,
  job_id varchar(64) not null references jobs(id),
  event_type varchar(32) not null,
  details text not null default '{}',
  created_at timestamp not null default current_timestamp
);

create table if not exists replay_copy_job_filters (
  id bigint generated by default as identity primary key,
  job_id varchar(64) not null references jobs(id),
  filter_value varchar(512) not null,
  created_at timestamp not null default current_timestamp
);

create index if not exists idx_replay_copy_job_filters_job_id
  on replay_copy_job_filters (job_id);

create table if not exists replay_copy_job_criteria (
  id bigint generated by default as identity primary key,
  job_id varchar(64) not null references jobs(id),
  row_index integer not null,
  field_name varchar(255) not null,
  field_value varchar(2048) not null,
  created_at timestamp not null default current_timestamp
);

create index if not exists idx_replay_copy_job_criteria_job_id
  on replay_copy_job_criteria (job_id, row_index);

create table if not exists replay_copy_job_search_results (
  id bigint generated by default as identity primary key,
  job_id varchar(64) not null references jobs(id),
  message_id varchar(255),
  message_key varchar(255),
  properties_json text not null default '{}',
  payload text not null default '',
  matched_field_value varchar(1024),
  created_at timestamp not null default current_timestamp
);

create index if not exists idx_replay_copy_job_search_results_job_id
  on replay_copy_job_search_results (job_id);
