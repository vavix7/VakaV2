import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from config import PUBLISH_HOUR, PUBLISH_MINUTE, PUBLISH_WARNING_HOURS, TIMEZONE

logger = logging.getLogger(__name__)


class WeeklyScheduler:
    """Надёжный недельный планировщик: предупреждение в воскресенье и публикация/закрытие недели в понедельник."""

    def __init__(self, database, warning_callback, publish_callback, rollover_callback=None):
        self.database = database
        self.warning_callback = warning_callback
        self.publish_callback = publish_callback
        self.rollover_callback = rollover_callback

        try:
            self.timezone = ZoneInfo(TIMEZONE)
        except Exception as e:
            logger.warning("Часовой пояс %s не найден: %s; использую UTC", TIMEZONE, e)
            self.timezone = ZoneInfo("UTC")

        self.last_warning_week = None
        self.last_publish_date = None
        self.last_rollover_date = None

    @staticmethod
    def _week_start_for_date(day):
        return day - timedelta(days=day.weekday())

    async def run(self):
        while True:
            try:
                now = datetime.now(self.timezone)
                await self._check_warning(now)
                await self._check_publication(now)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Ошибка планировщика")
            await asyncio.sleep(20)

    async def _check_warning(self, now):
        # Предупреждение относится к завершившейся в воскресенье неделе
        # и отправляется за PUBLISH_WARNING_HOURS до понедельничной публикации.
        if now.weekday() != 6:
            return

        publish_dt = datetime.combine(
            now.date() + timedelta(days=1),
            datetime.min.time(),
            tzinfo=self.timezone,
        ).replace(hour=PUBLISH_HOUR, minute=PUBLISH_MINUTE, second=0, microsecond=0)
        warning_dt = publish_dt - timedelta(hours=PUBLISH_WARNING_HOURS)

        if now < warning_dt or now >= publish_dt:
            return

        week_start = self._week_start_for_date(now.date()).isoformat()
        if self.last_warning_week == week_start:
            return

        # Если процесс перезапустился после времени предупреждения, всё равно
        # отправляем его один раз в течение воскресенья. Состояние БД защищает
        # от повторной отметки запроса.
        week = self.database.get_week(week_start)
        if not week:
            logger.info("Неделя %s ещё не создана — предупреждение пропущено", week_start)
            return
        if int(week["published"] or 0) != 0:
            self.last_warning_week = week_start
            return
        if week["publish_requested_at"]:
            self.last_warning_week = week_start
            return

        await self.warning_callback(week_start)
        self.last_warning_week = week_start
        logger.info("Отправлено предупреждение о публикации недели %s", week_start)

    async def _check_publication(self, now):
        # Rollover belongs to Monday 04:10 Moscow. If the process was down at
        # that moment, catch up on the first subsequent scheduler tick without
        # creating a fake Tuesday/Wednesday week.
        current_monday = self._week_start_for_date(now.date())
        rollover_dt = datetime.combine(
            current_monday, datetime.min.time(), tzinfo=self.timezone
        ).replace(hour=PUBLISH_HOUR, minute=PUBLISH_MINUTE, second=0, microsecond=0)
        if now < rollover_dt:
            return

        previous_start = current_monday - timedelta(days=7)
        previous_week = self.database.get_week(previous_start.isoformat())
        if previous_week and int(previous_week["rollover_done"] or 0) == 0:
            marker = current_monday.isoformat()
            if self.last_rollover_date != marker:
                self.last_rollover_date = marker
                if self.rollover_callback:
                    try:
                        await self.rollover_callback(marker)
                    except Exception:
                        logger.exception("Ошибка недельного rollover %s", marker)
                        self.last_rollover_date = None
                        return

        await self.publish_unpublished()

    async def publish_unpublished(self):
        today = datetime.now(self.timezone).date().isoformat()
        weeks = self.database.get_unpublished_completed_weeks(today)

        for week in weeks:
            try:
                await self.publish_callback(week["week_start"])
            except Exception:
                logger.exception("Ошибка публикации %s", week["week_start"])
                # Не блокируем следующие недели при единичной ошибке.
                continue
