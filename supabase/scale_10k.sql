-- TurboTeamV3 scale migration for 10k users.
-- Run this once in Supabase SQL Editor before deploying the app code that calls get_weekly_rating.

-- Indexes for hot read paths.
create index if not exists idx_activities_user_created_at
    on public.activities (user_id, created_at desc);

create index if not exists idx_activities_action_name
    on public.activities (action_name);

create unique index if not exists idx_users_telegram_user_id_unique
    on public.users (telegram_user_id);

create index if not exists idx_user_achievements_user_code
    on public.user_achievements (user_id, achievement_code);

create index if not exists idx_referrals_referrer_user_id
    on public.referrals (referrer_user_id);

create index if not exists idx_referrals_new_user_id
    on public.referrals (new_user_id);

-- Rebuild foreign keys with ON DELETE CASCADE so deleting one user row removes dependent data.
do $$
declare
    constraint_name text;
begin
    select tc.constraint_name into constraint_name
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema = kcu.table_schema
     and tc.table_name = kcu.table_name
    where tc.constraint_type = 'FOREIGN KEY'
      and tc.table_schema = 'public'
      and tc.table_name = 'activities'
      and kcu.column_name = 'user_id'
    limit 1;

    if constraint_name is not null then
        execute format('alter table public.activities drop constraint %I', constraint_name);
    end if;

    alter table public.activities
        add constraint activities_user_id_fkey
        foreign key (user_id) references public.users(id) on delete cascade;
end $$;

do $$
declare
    constraint_name text;
begin
    select tc.constraint_name into constraint_name
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema = kcu.table_schema
     and tc.table_name = kcu.table_name
    where tc.constraint_type = 'FOREIGN KEY'
      and tc.table_schema = 'public'
      and tc.table_name = 'user_achievements'
      and kcu.column_name = 'user_id'
    limit 1;

    if constraint_name is not null then
        execute format('alter table public.user_achievements drop constraint %I', constraint_name);
    end if;

    alter table public.user_achievements
        add constraint user_achievements_user_id_fkey
        foreign key (user_id) references public.users(id) on delete cascade;
end $$;

do $$
declare
    constraint_name text;
begin
    select tc.constraint_name into constraint_name
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema = kcu.table_schema
     and tc.table_name = kcu.table_name
    where tc.constraint_type = 'FOREIGN KEY'
      and tc.table_schema = 'public'
      and tc.table_name = 'referrals'
      and kcu.column_name = 'referrer_user_id'
    limit 1;

    if constraint_name is not null then
        execute format('alter table public.referrals drop constraint %I', constraint_name);
    end if;

    alter table public.referrals
        add constraint referrals_referrer_user_id_fkey
        foreign key (referrer_user_id) references public.users(id) on delete cascade;
end $$;

do $$
declare
    constraint_name text;
begin
    select tc.constraint_name into constraint_name
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema = kcu.table_schema
     and tc.table_name = kcu.table_name
    where tc.constraint_type = 'FOREIGN KEY'
      and tc.table_schema = 'public'
      and tc.table_name = 'referrals'
      and kcu.column_name = 'new_user_id'
    limit 1;

    if constraint_name is not null then
        execute format('alter table public.referrals drop constraint %I', constraint_name);
    end if;

    alter table public.referrals
        add constraint referrals_new_user_id_fkey
        foreign key (new_user_id) references public.users(id) on delete cascade;
end $$;

-- One SQL call for weekly rating: weekly HP + all-time referral count + rank.
create or replace function public.get_weekly_rating(
    p_period_start timestamptz,
    p_period_end timestamptz
)
returns table (
    telegram_user_id bigint,
    nick text,
    hp bigint,
    referrals_count bigint,
    rank bigint
)
language sql
stable
as $$
    with weekly_hp as (
        select
            a.user_id,
            coalesce(sum(a.hp_change), 0)::bigint as hp
        from public.activities a
        where a.created_at >= p_period_start
          and a.created_at < p_period_end
        group by a.user_id
    ),
    referral_counts as (
        select
            r.referrer_user_id as user_id,
            count(*)::bigint as referrals_count
        from public.referrals r
        group by r.referrer_user_id
    ),
    ranked as (
        select
            u.telegram_user_id::bigint as telegram_user_id,
            coalesce(u.nickname, 'ID:' || u.telegram_user_id::text) as nick,
            coalesce(wh.hp, 0)::bigint as hp,
            coalesce(rc.referrals_count, 0)::bigint as referrals_count,
            dense_rank() over (
                order by coalesce(wh.hp, 0) desc,
                         coalesce(u.nickname, 'ID:' || u.telegram_user_id::text) asc
            )::bigint as rank
        from public.users u
        left join weekly_hp wh on wh.user_id = u.id
        left join referral_counts rc on rc.user_id = u.id
    )
    select ranked.telegram_user_id, ranked.nick, ranked.hp, ranked.referrals_count, ranked.rank
    from ranked
    order by ranked.hp desc, ranked.nick asc;
$$;
