# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")
DAY_SWITCH_HOUR = 17


def resolve_end_calendar_day(
    now: datetime | None = None,
    switch_hour: int = DAY_SWITCH_HOUR,
) -> date:
    """
    日线目标日统一口径（北京时间）：
    - 17:00-23:59 -> T（当天）
    - 00:00-16:59 -> T-1（上一自然日）
    """
    dt = now.astimezone(CN_TZ) if now else datetime.now(CN_TZ)
    if dt.hour >= int(switch_hour):
        return dt.date()
    return (dt - timedelta(days=1)).date()

