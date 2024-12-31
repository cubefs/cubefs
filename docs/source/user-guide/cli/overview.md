# Overview

Using the command-line interface tool (CLI) can achieve convenient and fast cluster management. With this tool, you can view the status of the cluster and each node, and manage each node, volume, and user.

::: tip Note
With the continuous improvement of the CLI, 100% coverage of the interface functions of each node in the cluster will eventually be achieved.
:::

## Compilation and Configuration

After downloading the CubeFS source code, run the `build.sh` file in the `cubefs/cli` directory to generate the `cfs-cli` executable.

At the same time, a configuration file named `.cfs-cli.json` will be generated in the `root` directory. Modify the master address to the master address of the current cluster. You can also use the `./cfs-cli config info` and `./cfs-cli config set` commands to view and set the configuration file.

## Usage

In the `cubefs/cli` directory, run the command `./cfs-cli --help` or `./cfs-cli -h` to get the CLI help document.

The CLI is mainly divided into some types of management commands:

| Command               | Description               |
|-----------------------|---------------------------|
| cfs-cli cluster       | Cluster management        |
| cfs-cli metanode      | MetaNode management       |
| cfs-cli datanode      | DataNode management       |
| cfs-cli datapartition | Data Partition management |
| cfs-cli metapartition | Meta Partition management |
| cfs-cli config        | Configuration management  |
| cfs-cli volume, vol   | Volume management         |
| cfs-cli user          | User management           |
| cfs-cli nodeset       | Nodeset management        |
| cfs-cli quota         | Quota management          |
