from treasury_yield_analysis.task import Treasury_Yield_Task, Mariadb_Task, BEA_Task, Yfinance_Task
from treasury_yield_analysis.datasource import Tyr_DS, Mariadb_DS, Bea_DS, Yfinance_DS
import platform

app_os = platform.system()

treasury_yield_ds = Tyr_DS("https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?")
treasury_yield_obj = Treasury_Yield_Task(treasury_yield_ds)
tyr_output = treasury_yield_obj._execute('ALL')

mariadb_ds = Mariadb_DS('root', 'root', 'localhost', 'tyr_analysis', app_os)

mariadb_task = Mariadb_Task(mariadb_ds)
mariadb_task.insert_treasury_yields(tyr_output)

bea_ds = Bea_DS("https://apps.bea.gov/api/data/?")
bea_gdp_obj = BEA_Task(bea_ds)

bea_output = bea_gdp_obj._execute(1990, 2021)
mariadb_task.insert_us_q_gdp(bea_output)
'''
yfinance_ds = Yfinance_DS("^GSPC")
yfinance_obj = Yfinance_Task(yfinance_ds, ['Open', 'Close'])

yfinance_output = yfinance_obj._execute("1990-01-01", "2021-07-01")
mariadb_task.insert_stock_market_performance(yfinance_output)

mariadb_task._mariadb_DS._conn.close()'''
