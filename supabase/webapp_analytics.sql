-- TurboTeamV3 Mini App analytics table.
-- Run this once in Supabase SQL Editor before deploying the app code that calls log_webapp_event.

create table if not exists public.webapp_events (
    id bigint generated always as identity primary key,
    telegram_user_id bigint not null,
    event_name text not null,
    meta jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_webapp_events_telegram_user_id_created_at
    on public.webapp_events (telegram_user_id, created_at desc);

create index if not exists idx_webapp_events_event_name_created_at
    on public.webapp_events (event_name, created_at desc);
