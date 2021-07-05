from treasury_yield_analysis.task import Treasury_Yield_Task, Mariadb_Task, BEA_Task
from treasury_yield_analysis.datasource import Tyr_DS, Mariadb_DS, Bea_DS


treasury_yield_ds = Tyr_DS("https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?")
treasury_yield_obj = Treasury_Yield_Task(treasury_yield_ds)

tyr_output = treasury_yield_obj._execute('ALL')
#print(output)
mariadb_ds = Mariadb_DS('root', 'root', 'localhost', 'us_treasury_yield_rates')
#test_md = mariadb_ds.retrieve_all_treasury_records()
#print(test_md)
#mariadb_ds.insert_treasury_records(output)
mariadb_task = Mariadb_Task(mariadb_ds)
#mariadb_task.insert_treasury_yields(tyr_output)

bea_ds = Bea_DS("https://apps.bea.gov/api/data/?")
bea_gdp_obj = BEA_Task(bea_ds)

bea_output = bea_gdp_obj._execute(1990, 2021)
mariadb_task.insert_us_q_gdp(bea_output)