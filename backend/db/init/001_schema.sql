create table if not exists environments (
  id varchar(32) primary key,
  name varchar(64) not null,
  kind varchar(16) not null,
  region varchar(64) not null,
  cluster_label varchar(64) not null,
  broker_url varchar(255) not null,
  admin_url varchar(255) not null,
  status varchar(16) not null,
  created_at timestamp default current_timestamp
);

create table if not exists jobs (
  id varchar(64) primary key,
  type varchar(32) not null,
  environment_id varchar(32) not null references environments(id),
  status varchar(16) not null,
  parameters jsonb not null default '{}'::jsonb,
  created_at timestamp not null default current_timestamp,
  updated_at timestamp not null default current_timestamp
);

create table if not exists job_events (
  id bigserial primary key,
  job_id varchar(64) not null references jobs(id),
  event_type varchar(32) not null,
  details jsonb not null default '{}'::jsonb,
  created_at timestamp not null default current_timestamp
);
