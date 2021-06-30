from treasury_yield_records.task import Treasury_Yield_Task, Mariadb_Task
from treasury_yield_records.datasource import Tyr_DS, Mariadb_DS


treasury_yield_ds = Tyr_DS("https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?")
treasury_yield_obj = Treasury_Yield_Task(treasury_yield_ds)

output = treasury_yield_obj._execute(2021)
#print(output)
mariadb_ds = Mariadb_DS('root', 'root', 'localhost', 'us_treasury_yield_rates')
#test_md = mariadb_ds.retrieve_all_treasury_records()
#print(test_md)
#mariadb_ds.insert_treasury_records(output)
mariadb_task = Mariadb_Task(mariadb_ds)
mariadb_task.insert_treasury_yields(output)