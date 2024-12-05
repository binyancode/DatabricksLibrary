from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

class Functions:
    def generate_day_range_path(target_date, range_days=3):
        # 获取指定日期以及之前3天的日期
        dates = [target_date - timedelta(days=i) for i in range(range_days + 1)]
        
        # 提取年、月、日
        years = {date.year for date in dates}
        months = {date.month for date in dates}
        days = {date.day for date in dates}
        
        # 将年、月、日分别用大括号列出来，如果只有一个值则不加大括号
        year_str = f"year={{{','.join(map(str, sorted(years)))}}}" if len(years) > 1 else f"year={next(iter(years))}"
        month_str = f"month={{{','.join(map(lambda x: f'{x:02}', sorted(months)))}}}" if len(months) > 1 else f"month={next(iter(months)):02}"
        day_str = f"day={{{','.join(map(lambda x: f'{x:02}', sorted(days)))}}}" if len(days) > 1 else f"day={next(iter(days)):02}"
        
        # 拼装最终的字符串
        path = f"{year_str}/{month_str}/{day_str}"

        return path