# -*- coding: utf-8 -*-
"""
Token 持久化到 localStorage，实现刷新页面后登录态保持。
依赖 streamlit-javascript 在浏览器端读写 localStorage。
"""
from __future__ import annotations

import json
_STORAGE_KEY_ACCESS = "akshare_access_token"
_STORAGE_KEY_REFRESH = "akshare_refresh_token"


def restore_tokens_from_storage() -> tuple[str | None, str | None]:
    """从 localStorage 读取 token，返回 (access_token, refresh_token)，无则返回 (None, None)。

    注意：streamlit-javascript 的 st_javascript() 是异步的——第一次渲染时
    返回 0（占位），第二次 rerun 才拿到真实值。
    调用方应在拿到 (None, None) 后不要标记"已尝试"，而是允许下一次 rerun 重试。
    """
    try:
        from streamlit_javascript import st_javascript

        js = (
            "JSON.stringify({"
            f'access_token: localStorage.getItem("{_STORAGE_KEY_ACCESS}") || "", '
            f'refresh_token: localStorage.getItem("{_STORAGE_KEY_REFRESH}") || ""'
            "})"
        )
        result = st_javascript(js)
        # st_javascript 首次渲染返回 0（int），需等下一次 rerun 才拿到真实字符串
        if not result or not isinstance(result, str) or result == "0":
            return (None, None)
        data = json.loads(result)
        access = (data.get("access_token") or "").strip()
        refresh = (data.get("refresh_token") or "").strip()
        if access and refresh:
            return (access, refresh)
        return (None, None)
    except Exception:
        return (None, None)


def persist_tokens_to_storage(access_token: str, refresh_token: str) -> bool:
    """将 token 写入 localStorage，成功返回 True。"""
    if not access_token or not refresh_token:
        return False
    try:
        from streamlit_javascript import st_javascript

        # json.dumps 确保字符串安全转义
        a = json.dumps(access_token)
        r = json.dumps(refresh_token)
        js = (
            f'localStorage.setItem("{_STORAGE_KEY_ACCESS}", {a}); '
            f'localStorage.setItem("{_STORAGE_KEY_REFRESH}", {r}); '
            'return "ok";'
        )
        st_javascript(js)
        return True
    except Exception:
        return False


def clear_tokens_from_storage() -> bool:
    """清除 localStorage 中的 token，成功返回 True。"""
    try:
        from streamlit_javascript import st_javascript

        js = (
            f'localStorage.removeItem("{_STORAGE_KEY_ACCESS}"); '
            f'localStorage.removeItem("{_STORAGE_KEY_REFRESH}"); '
            'return "ok";'
        )
        st_javascript(js)
        return True
    except Exception:
        return False
