# MetaData performance evaluation

The results of metadata performance testing through [mdtest](https://github.com/LLNL/mdtest) are as follows:

**tool settings**

``` bash
#!/bin/bash
TEST_PATH=/mnt/cfs/mdtest # mount point of CubeFS volume
for CLIENTS in 1 2 4 8 # number of clients
do
mpirun --allow-run-as-root -np $CLIENTS --hostfile hfile01 mdtest -n 5000 -u -z 2 -i 3 -d $TEST_PATH;
done
```

## Directory Creation

![Dir Creation](../pic/cfs-mdtest-dir-creation.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 769.908   | 3630.337    | 12777.619    | 20629.592    |
| 2 Clients | 1713.038  | 7259.282    | 24064.052    | 36769.599    |
| 4 Clients | 3723.993  | 14002.366   | 42976.837    | 61513.648    |
| 8 Clients | 6681.783  | 23946.143   | 64191.38     | 93729.222    |

## Directory Removal

![Dir Removal](../pic/cfs-mdtest-dir-removal.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 853.995   | 4012.404    | 15238.647    | 42028.845    |
| 2 Clients | 1906.261  | 7967.688    | 28410.308    | 62338.506    |
| 4 Clients | 3942.590  | 15601.799   | 46741.945    | 87411.655    |
| 8 Clients | 7072.080  | 25183.092   | 69054.923    | 79459.091    |

## Directory Status Check

![Dir Stat](../pic/cfs-mdtest-dir-stat.png)

|           | 1 Process   | 4 Processes | 16 Processes | 64 Processes |
|-----------|-------------|-------------|--------------|--------------|
| 1 Client  | 462527.445  | 1736760.332 | 6194206.768  | 15509755.836 |
| 2 Clients | 885454.335  | 3414538.352 | 12263175.104 | 24951003.498 |
| 4 Clients | 1727030.782 | 6874284.765 | 24371306.250 | 10412238.894 |
| 8 Clients | 1897588.214 | 7499219.744 | 25927923.646 | 4264896.279  |

## File Creation

![File Creation](../pic/cfs-mdtest-file-creation.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 706.453   | 3306.270    | 9647.176     | 10879.290    |
| 2 Clients | 1601.891  | 6601.522    | 19181.965    | 20756.693    |
| 4 Clients | 3369.911  | 13165.056   | 36158.061    | 41817.753    |
| 8 Clients | 6312.911  | 22560.687   | 55801.062    | 76157.675    |

## File Removal

![File Removal](../pic/cfs-mdtest-file-removal.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 1220.322  | 4606.340    | 11715.457    | 23653.043    |
| 2 Clients | 2360.133  | 8971.361    | 22097.206    | 41579.985    |
| 4 Clients | 4547.021  | 17242.900   | 34965.471    | 61726.902    |
| 8 Clients | 8809.381  | 20379.839   | 55363.389    | 71101.736    |

## Tree Creation

![Tree Creation](../pic/cfs-mdtest-tree-creation.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 734.383   | 379.403     | 146.070      | 37.811       |
| 2 Clients | 648.938   | 432.150     | 148.921      | 30.699       |
| 4 Clients | 756.639   | 394.733     | 123.722      | 23.998       |
| 8 Clients | 607.547   | 263.439     | 124.911      | 7.510        |

## Tree Removal

![Tree Removal](../pic/cfs-mdtest-tree-removal.png)

|           | 1 Process | 4 Processes | 16 Processes | 64 Processes |
|-----------|-----------|-------------|--------------|--------------|
| 1 Client  | 552.706   | 83.197      | 23.823       | 5.747        |
| 2 Clients | 448.557   | 84.633      | 24.037       | 5.137        |
| 4 Clients | 453.520   | 85.636      | 23.490       | 5.233        |
| 8 Clients | 429.920   | 83.449      | 23.777       | 1.667        |