-- Гарантує, що за одне тренування (одне повідомлення в групі) HP-бонус
-- за реакції нараховується максимум один раз, навіть якщо логіка в коді
-- спрацює повторно (наприклад, кількість реакцій зросла з 3 до 4).
create unique index if not exists uq_activities_reaction_bonus_video_id
    on public.activities (video_id)
    where action_name = 'Reaction Bonus';
