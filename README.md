<div align="center">

# 🏋️‍♂️ TurboTeam Bot

### ⚡ Telegram fitness game bot with HP, video proof, ratings and community discipline

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/aiogram-3.x-00AEEF?style=for-the-badge" />
  <img src="https://img.shields.io/badge/supabase-database-3ECF8E?style=for-the-badge&logo=supabase" />
  <img src="https://img.shields.io/badge/redis-cache-red?style=for-the-badge&logo=redis" />
  <img src="https://img.shields.io/badge/status-active-success?style=for-the-badge" />
</p>

<p align="center">
  <b>Train → Send Proof → Earn HP → Climb Rating → Stay Disciplined</b>
</p>

---

<img src="https://media.giphy.com/media/l0MYt5jPR6QX5pnqM/giphy.gif" width="430"/>

</div>

---

# 🚀 About the project

**TurboTeam Bot** is a Telegram bot that turns a fitness group into a competitive discipline game.

The main idea is simple:

> users train, send video proof, earn HP points, compete in weekly rankings, invite friends, unlock levels and stay active together.

The bot is designed for Telegram fitness communities where discipline matters more than empty promises.  
Every real action is tracked, verified, scored and shown in the group.

---

# 🔥 Core idea

TurboTeam is not just a chat.

It is a small fitness ecosystem inside Telegram:

- users choose an activity;
- the bot asks for a video note as proof;
- the activity is saved in the database;
- HP points are added;
- the report is posted to the group;
- the rating updates;
- inactive users get reminders;
- fake reports can be rejected;
- the most active users compete for the top.

---

# ⚙️ Main features

## 🏋️ Training actions

Users can choose:

- 🏋️ **Gym**
- 🦾 **Street**
- 🧘 **Rest**
- 🚫 **Skip**

Gym and Street require a Telegram video note as proof.

---

## 📹 Video proof system

For real training actions, the user must send a fresh video note.

The bot checks and controls:

- video note flow;
- active training session;
- session timeout;
- repeated actions;
- fake or incorrect reports;
- forwarded videos;
- duplicate activity for the same day.

After successful proof, the bot posts the report to the group.

---

## ⚡ HP system

Each activity gives or removes HP.

Example logic:

- Gym / Street — training HP with bonus range;
- Rest — small positive HP;
- Skip — penalty or negative action;
- Welcome Bonus — first registration reward;
- Streak Bonus — extra HP for consistent training;
- Rollback — removes HP after rejected fake/invalid training.

---

## 🏆 Weekly rating

The bot builds a weekly leaderboard.

The TurboTeam week starts on:

```text
Sunday 20:00 Kyiv time