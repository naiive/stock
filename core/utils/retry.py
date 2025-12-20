# -*- coding: utf-8 -*-
import time
import functools

def retry(max_retries=3, delay=2):
    """
    通用重试装饰器
    :param max_retries: 最大重试次数
    :param delay: 每次重试间的休眠秒数
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    print(f"⚠️ [重试] {func.__name__} 失败 (第 {attempt}/{max_retries} 次): {e}")
                    if attempt < max_retries:
                        time.sleep(delay)
            print(f"❌ [失败] {func.__name__} 超过最大重试次数")
            raise last_exception
        return wrapper
    return decorator