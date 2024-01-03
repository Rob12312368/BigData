import pandas as pd
import matplotlib.pyplot as plt
import json

data = {}
for i in range(4):
    try:
        with open(f'/files/partition-{i}.json', 'r') as f:
            data[i] = json.loads(f.read())
    except Exception as e:
        pass
plotdata = {}
for d in data.values():
    if 'January' in d:
        lastyear = max(d['January'].keys())
        print(lastyear)
        plotdata['January'+'-'+lastyear] = d['January'][lastyear]['avg']
    if 'February' in d:
        lastyear = max(d['February'].keys())
        plotdata['February'+'-'+lastyear] = d['February'][lastyear]['avg']
    if 'March' in d:
        lastyear = max(d['March'].keys())
        plotdata['March'+'-'+lastyear] = d['March'][lastyear]['avg']

        
print(plotdata)
month_series = pd.Series(plotdata)
print(month_series)
fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
plt.tight_layout()
plt.savefig("/files/month.svg")

print('end')