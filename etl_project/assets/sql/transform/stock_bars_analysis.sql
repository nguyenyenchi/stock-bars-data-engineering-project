
drop table if exists stock_bars_analysis;
create table stock_bars_analysis as
with prev_close AS (
select 
	stock, 
	company,
	timestamp,
	close,
	LAG(close, 1) OVER (PARTITION BY stock ORDER BY timestamp) AS prev_close
from stock_bars
),

daily_returns AS (
select 
	stock,
	company,
	(timestamp::timestamp)::date AS date,
	close,
	prev_close,
	ROUND(((close - prev_close) / nullif(prev_close, 0))::numeric, 3) AS daily_return
	
from prev_close)

select 
	stock, 
	company,
	date,
	close,
	prev_close,
	ROUND(daily_return * 100, 1) as daily_return_pct,
	ROUND((AVG(close) OVER (PARTITION BY stock ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW))::numeric, 2) AS moving_avg_5_day,
	ROUND((STDDEV(daily_return) OVER (PARTITION BY stock ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW))::numeric, 2) AS stddev_5_day

from daily_returns;
	