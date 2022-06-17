from traderbase.data_source.interactivebrokers import InteractiveBrokers

p = {
    'symbol': 'AUDUSD',
    'min_bar_interval': '1d',
    'start_year': '2020',
    'start_month': '01',
    'start_day': '01',
    'end_year': '2021',
    'end_month': '01',
    'end_day': '01',
}

ib = InteractiveBrokers(p)
df = ib.fetch()
print(df)