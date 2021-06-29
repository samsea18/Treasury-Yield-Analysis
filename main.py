from treasury_yield_records.task import treasury_yield_task
from treasury_yield_records.datasource import tyrDS


treasury_yield_ds = tyrDS("https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?")
treasury_yield_obj = treasury_yield_task(treasury_yield_ds)

output = treasury_yield_obj._execute(2021)

print(output)