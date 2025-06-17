import os
import sys
import json

import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.use('Agg')

sizes = {
    4 << 10: "4K",
    32 << 10: "32K",
    128 << 10: "128K",
    1 << 20: "1M",
}

result_file = "result.json"
results = []
for p in os.listdir("."):
    if p.startswith("result_") and \
        not p.startswith("result_connection_") and \
        not p.startswith("result_concurrence_") and \
            not p.startswith("result_crc_"):
        print(p)
        with open(p) as f:
            results.extend(json.loads(str(f.read())))

with open(result_file, "w") as f:
    f.write(json.dumps(results))
df = pd.read_json(result_file)
df.to_excel('result.xlsx', index=False)


mode = sys.argv[1]

results.sort(key=lambda x:
             (x['mode'], x['procs'], x['connection'],
              x['concurrence'], x['requestsize']))
x = []
y = []
for r in results:
    if r['mode'] == mode and not r['writev']:
        x.append("{0}-{1}-{2}-{3}".format(
            r['procs'], r['connection'], r['concurrence'],
            sizes[r['requestsize']]))
        y.append(r['speed'] / (1 << 20))

if mode == "tcp":
    plt.figure().set_size_inches(50, 15)
else:
    plt.figure().set_size_inches(50, 15)

plt.bar(x, y, color=['blue', 'green', 'brown', 'olive'])
plt.grid(which='both', axis='y')
plt.title(mode)
plt.xticks(rotation=90)
plt.minorticks_on()
plt.tick_params(axis='y', which='minor', length=10)
plt.xlabel("(procs-connection-concurrence-requestsize)")
plt.ylabel("speed(MB/s)")
plt.savefig("p_"+mode+".png")
