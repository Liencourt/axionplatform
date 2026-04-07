from .base import BaseCollector, SinalColetado
from .google_trends import GoogleTrendsCollector
from .newsapi import NewsApiCollector, RSSCollector
from .reddit import RedditCollector

__all__ = [
    "BaseCollector",
    "SinalColetado",
    "GoogleTrendsCollector",
    "NewsApiCollector",
    "RSSCollector",
    "RedditCollector",
]
