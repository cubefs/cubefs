import os
import json

import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.use('Agg')

sizes = {
    4 << 10: "4K",
    32 << 10: "32K",
    128 << 10: "128K",
    1 << 20: "1M",
}

results = []
for p in os.listdir("."):
    if p.startswith("result_connection_"):
        print(p)
        with open(p) as f:
            results.extend(json.loads(str(f.read())))

results.sort(key=lambda x:
             (x['procs'], x['connection'], x['concurrence'],
              x['requestsize'], x['mode']))
x = []
y = []
for r in results:
    n = r['procs']
    if r['connection'] == n and r['concurrence'] == 1 and not r['writev']:
        x.append("{0}-{1}-{2}-{3}".format(
            r['procs'], r['connection'], sizes[r['requestsize']], r['mode']))
        y.append(r['speed'] / (1 << 20))

plt.figure().set_size_inches(50, 15)

plt.bar(x, y, color=['blue', 'green', 'brown', 'olive'])
plt.grid(which='both', axis='y')
plt.title("connection")
plt.xticks(rotation=90)
plt.minorticks_on()
plt.tick_params(axis='y', which='minor', length=10)
plt.xlabel("(procs-connection-requestsize-mode)")
plt.ylabel("speed(MB/s)")
plt.savefig("p_connection.png")
