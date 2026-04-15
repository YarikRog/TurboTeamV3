<div align="center">

# 🏋️‍♂️ Fitness Game Telegram Bot

### ⚡ Гейміфікована система активності в Telegram

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10+-blue?style=for-the-badge&logo=python" />
  <img src="https://img.shields.io/badge/aiogram-3.x-00AEEF?style=for-the-badge" />
  <img src="https://img.shields.io/badge/redis-cache-red?style=for-the-badge&logo=redis" />
  <img src="https://img.shields.io/badge/status-active-success?style=for-the-badge" />
</p>

<p align="center">
  <b>Активність → HP → Рейтинг → Змагання</b>
</p>

---

<img src="https://media.giphy.com/media/3o7aD2saalBwwftBIY/giphy.gif" width="400"/>

</div>

---

# 🚀 Про проєкт

Це Telegram-бот, який перетворює звичайні дії користувачів у **ігрову систему прогресу**.

Кожна активність = очки (HP), які впливають на рейтинг і статус користувача.

---

# ⚙️ Функціонал

## 🏋️ Активності
- Gym тренування
- Street тренування
- Відпочинок / пропуск

## 📹 Відео-підтвердження
- Обов’язковий video note
- Перевірка:
  - ❌ переслані відео
  - ❌ старі записи
  - ❌ спам / флуд
- Прив’язка до останньої дії

## 🚫 Анти-спам система
- Redis rate limit
- Лічильник натискань
- Автоблок при спамі

## 🚩 Скарги (Community Moderation)
- Користувачі голосують 🚩
- 5 скарг → санкція
- Автоматичне списання HP

## 🏆 Рейтинг
- Топ користувачів
- Система мотивації
- Конкуренція між учасниками

---

# 🧠 Архітектура

```text
Telegram → Aiogram Router
                ↓
         Service Layer
   (ActivityService / Logic)
                ↓
     Redis (cache + anti-spam)
                ↓
       Database (HP / users)