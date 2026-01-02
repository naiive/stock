#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import datetime

class LogRedirector:
    """
    日志重定向工具：同时将 stdout 输出到终端和按日期生成的文件中。
    """
    def __init__(self, log_folder="logs"):
        # 路径：project_root/logs/YYYYMMDD/
        self.today_str = datetime.datetime.now().strftime('%Y%m%d')
        self.log_dir = os.path.join(log_folder, self.today_str)
        os.makedirs(self.log_dir, exist_ok=True)

        self.terminal = sys.stdout
        timestamp = datetime.datetime.now().strftime('%H%M%S')
        self.log_path = os.path.join(self.log_dir, f"{timestamp}.log")
        self.log_file = open(self.log_path, 'a', encoding='utf-8')

    def write(self, message):
        # 输出到控制台
        self.terminal.write(message)
        self.terminal.flush()
        # 输出到文件
        if self.log_file:
            self.log_file.write(message)
            self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        if self.log_file:
            self.log_file.flush()

    def __enter__(self):
        sys.stdout = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self.terminal
        if self.log_file:
            # 程序结束前打印日志位置
            print(f"\n📄 本次运行日志已保存至: {self.log_path}")
            self.log_file.close()