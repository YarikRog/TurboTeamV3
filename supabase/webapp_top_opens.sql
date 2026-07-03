-- Run this once in Supabase SQL Editor before deploying the bot code that calls
-- get_top_webapp_opens (used by the /webapp admin command).

create or replace function public.get_top_webapp_opens(
    p_period_start timestamptz,
    p_limit int default 10
)
returns table (
    telegram_user_id bigint,
    nickname text,
    open_count bigint
)
language sql
stable
as $$
    select
        e.telegram_user_id::bigint as telegram_user_id,
        coalesce(u.nickname, 'ID:' || e.telegram_user_id::text) as nickname,
        count(*)::bigint as open_count
    from public.webapp_events e
    left join public.users u on u.telegram_user_id = e.telegram_user_id
    where e.event_name = 'app_open'
      and e.created_at >= p_period_start
    group by e.telegram_user_id, u.nickname
    order by open_count desc
    limit p_limit
$$;
