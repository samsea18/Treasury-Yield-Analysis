from treasury_yield_records.task import Treasury_Yield_Task
from treasury_yield_records.datasource import TyrDS, Mariadb_DS


treasury_yield_ds = TyrDS("https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?")
treasury_yield_obj = Treasury_Yield_Task(treasury_yield_ds)

output = treasury_yield_obj._execute(2021)

mariadb_ds = Mariadb_DS('root', 'root', 'localhost', 'us_treasury_yield_rates')
test_md = mariadb_ds.retrieve_all_treasury_recs()
print(test_md)