# spiders/antibot.py
from __future__ import annotations
import random
import time
from dataclasses import dataclass
from typing import Any, Optional, Tuple
from bs4 import BeautifulSoup

"""反爬虫检测异常（可恢复/不可恢复由策略决定）"""
class AntiBotDetectedException(Exception):
    pass

"""致命反爬触发：应立即终止整个程序"""
class FatalAntiBotException(Exception):
    pass


@dataclass
class AntiBotConfig:
    # --- detection ---
    min_html_length: int = 800
    min_body_text_length: int = 50
    antibot_patterns: Tuple[str, ...] = (
        "验证码", "captcha", "访问限制", "rate limit", "访问异常",
        "安全验证", "请完成验证",  "robot check",
        "security check",
        "为了保障您的访问安全", "检测到异常访问", "请完成下方验证后继续",
        "您的请求过于频繁", "请稍后再试", "请输入验证码继续访问",
    )
    antibot_titles: Tuple[str, ...] = (
        "验证", "captcha", "安全验证", "访问限制","access denied",
        "robot check", "verification required",
    )
    captcha_selectors: Tuple[str, ...] = (
        ".captcha", ".verification-code", ".security-check", "#captcha",
        ".recaptcha", ".h-captcha", ".g-recaptcha", ".verify-code",
        ".verification", ".verification-modal", ".antibot-modal",
        ".antispam", ".human-verification", ".robot-check",
    )
    bad_title_keywords: Tuple[str, ...] = ("404", "无法访问", "出错了")

    # --- circuit breaker ---
    consecutive_threshold: int = 3
    cooldown_range: Tuple[int, int] = (60, 180)

    # --- strategy ---
    # "cooldown": 冷却+（可选）换代理+重启driver+返回None
    # "fatal":    直接抛 FatalAntiBotException 让上层退出
    mode: str = "cooldown"


class AntiBotDetector:
    def __init__(self, cfg: AntiBotConfig):
        self.cfg = cfg

    def detect(self, *, soup: Optional[BeautifulSoup], html: str, html_length: int) -> None:
        """
        如果检测到反爬/风控，直接 raise AntiBotDetectedException。
        """
        cfg = self.cfg

        # 1) short html
        if html_length < int(cfg.min_html_length):
            raise AntiBotDetectedException(f"Page source too short: {html_length}")

        if soup is None:
            # 没 soup 也当作异常（保守）
            raise AntiBotDetectedException("Soup is None (possible blocked/empty)")

        # 只取可见文本，避免 script/style 中的“captcha”等误杀
        visible_text = soup.get_text(" ", strip=True).lower()

        # title 单独取
        title = ""
        try:
            title = (soup.title.string if soup.title else "") or ""
        except Exception:
            title = ""
        title_l = title.lower()

        # 2) patterns（在可见文本 + title 中匹配）
        haystack = visible_text + " " + title_l
        for pat in cfg.antibot_patterns:
            if pat.lower() in haystack:
                raise AntiBotDetectedException(f"Anti-bot keyword hit: {pat}")

        # 3) title keywords / titles
        title = ""
        try:
            title = (soup.title.string if soup.title else "") or ""
        except Exception:
            title = ""
        title_l = title.lower()

        for k in cfg.bad_title_keywords:
            if k.lower() in title_l:
                raise AntiBotDetectedException(f"Bad page title: {title}")

        for t in cfg.antibot_titles:
            if t.lower() in title_l:
                raise AntiBotDetectedException(f"Anti-bot title hit: {title}")

        # 4) captcha selectors
        for sel in cfg.captcha_selectors:
            if soup.select_one(sel):
                raise AntiBotDetectedException(f"Captcha element hit: {sel}")

        # 5) body too short
        body = soup.find("body")
        if body:
            bt = body.get_text(strip=True)
            if len(bt) < int(cfg.min_body_text_length):
                raise AntiBotDetectedException(f"Body text too short: {len(bt)}")


class AntiBotHandler:
    """
    处理策略：cooldown / fatal
    由 BaseSpider 在 except AntiBotDetectedException 时调用。
    """

    def __init__(self, cfg: AntiBotConfig):
        self.cfg = cfg

    def handle(
        self,
        *,
        logger: Any,
        url: str,
        consecutive_count: int,
        rotate_proxy_fn: Optional[callable] = None,
        restart_driver_fn: Optional[callable] = None,
        close_driver_fn: Optional[callable] = None,
    ) -> None:
        cfg = self.cfg

        # 未达到阈值：让上层继续走 retry（或你也可以直接当 fatal/cooldown）
        if consecutive_count < int(cfg.consecutive_threshold):
            logger.warning(
                f"[anti-bot] detected but below threshold ({consecutive_count}/{cfg.consecutive_threshold}): {url}"
            )
            return

        # 达到阈值：按策略处理
        if cfg.mode == "fatal":
            logger.critical(f"[FATAL] Anti-bot threshold reached ({consecutive_count}). url={url}")
            if close_driver_fn:
                try:
                    close_driver_fn()
                except Exception:
                    pass
            raise FatalAntiBotException(f"Anti-bot triggered at {url}")

        # default: cooldown
        cooldown = cfg.cooldown_range
        sleep_s = random.uniform(float(cooldown[0]), float(cooldown[1]))
        logger.error(
            f"[anti-bot] threshold reached ({consecutive_count}). cooldown {sleep_s:.1f}s. url={url}"
        )

        if rotate_proxy_fn:
            try:
                rotate_proxy_fn()
            except Exception:
                pass

        if restart_driver_fn:
            try:
                restart_driver_fn("anti-bot cooldown")
            except Exception:
                pass

        time.sleep(sleep_s)
