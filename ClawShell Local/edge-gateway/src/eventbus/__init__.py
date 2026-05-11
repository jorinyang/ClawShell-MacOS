"""
EventBus - ClawShell Edge Gateway
================================

统一事件总线模块。
提供事件的发布、订阅、路由和持久化功能。

导出:
    EventBus - 事件总线核心类
    Event - 统一事件格式
    EventType - 事件类型枚举
    EventFilter - 事件过滤器
    get_eventbus - 获取全局事件总线实例
    publish - 快捷发布函数
    subscribe - 快捷订阅函数
"""

from .core import EventBus, get_eventbus, publish, subscribe, configure_eventbus
from .schema import Event, EventType, EventSource, EventFilter

__all__ = [
    "EventBus",
    "Event",
    "EventType", 
    "EventSource",
    "EventFilter",
    "get_eventbus",
    "publish",
    "subscribe",
    "configure_eventbus",
]

__version__ = "0.1.0"
