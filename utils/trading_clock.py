# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")
try:
    DAY_SWITCH_HOUR = int(os.getenv("MARKET_DATA_READY_HOUR", "16"))
except Exception:
    DAY_SWITCH_HOUR = 16


def resolve_end_calendar_day(
    now: datetime | None = None,
    switch_hour: int = DAY_SWITCH_HOUR,
) -> date:
    """
    日线目标日统一口径（北京时间）：
    - switch_hour(默认16):00 - 23:59 -> T（当天）
    - 00:00 - switch_hour(默认16):59 -> T-1（上一自然日）
    """
    dt = now.astimezone(CN_TZ) if now else datetime.now(CN_TZ)
    if dt.hour >= int(switch_hour):
        return dt.date()
    return (dt - timedelta(days=1)).date()
