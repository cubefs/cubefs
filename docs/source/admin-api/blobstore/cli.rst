CLI
===

Using the Command Line Interface (CLI) can manage the cluster conveniently. With this unix-like command tool, you can view the status of the cluster and connect api of each module, etc.

As the CLI continues to improve, it will eventually achieve 100% coverage of the APIs of each module.

.. code-block:: txt

    1. the tool is auto-completion.
    2. most data type display as readable.


Compile and Configure
---------------------

Run ``make cli`` at main directory of our project, ``cli`` built in ./bin directory.

``./bin/cli -c cli.conf`` will start an unix-like command tool, the flag ``-c cli.conf`` is optional.
It mainly includes the configuration of some common variables, such as ``access`` layer service discovery address,
``clustermgr`` service address and so on.

.. code-block:: json

    {
        "access": {
            "conn_mode": 4,
            "priority_addrs": [
                "http://localhost:9500"
            ]
        },
        "default_cluster_id": 1,
        "cm_cluster": {
            "1": "http://127.0.0.1:9998 http://127.0.0.1:9999 http://127.0.0.1:10000"
        }
    }


Usage
-----

Cli run as unix command, like:

.. code-block:: bash

    cli MainCmd SubCmd [Cmd ...] [--flag Val ...] -- [-arg ...]

    1 #$> ./cli config set conf-key conf-val
    To set Key: conf-key Value: conf-val

    2 #$> ./cli util time
    timestamp = 1640156245364981202 (seconds = 1640156245 nanosecs = 364981202)
            --> format: 2021-12-22T14:57:25.364981202+08:00 (now)

``./bin/cli`` Start an unix-like command tool program。

.. code-block:: txt

    'help' show all completed implementations of some modules.
    we recommend the behavior as `cmd subCmd ... --flag -- -arg`.

CLI mainly implemented commands blew:

.. csv-table::
   :header: "Command", "description"

   "cli config", "Manage configurations of the cli program"
   "cli util", "Utils, like: location parser, time parser and so on"
   "cli access", "File upload, downlod and delete"
   "cli cm", "Manage cluster components"
   "cli scheduler", "background tasks management"
   "cli ...", "continuing ..."


Config
------

.. code-block:: bash

    manager memory cache of config

    Usage:
      config [flags]

    Sub Commands:
      del   del config of keys
      get   get config in cache
      set   set config to cache
      type  print type in cache


Util
------------

.. code-block:: bash

    util commands, parse everything

    Usage:
      util [flags]

    Sub Commands:
      location  parse location <[json | hex | base64]>
      redis     redis tools
      time      time format [unix] [format]
      token     parse token <token>
      vuid      parse vuid <vuid>


Access
------

.. code-block:: bash

    blobstore access api tools

    Usage:
      access [flags]

    Sub Commands:
      cluster  show cluster
      del      del file
      ec       show ec buffer size
      get      get file
      put      put file


Clustermgr
----------

.. code-block:: bash

    cluster manager tools

    Usage:
      cm [flags]

    Sub Commands:
      cluster    cluster tools
      config     config tools
      disk       disk tools
      listAllDB  list all db tools
      service    service tools
      stat       show stat of clustermgr
      volume     volume tools
      wal        wal tools


Scheduler
---------

.. code-block:: bash

    scheduler tools

    Usage:
      scheduler [flags]

    Flags:
      -h, --help     display help

    Sub Commands:
      checkpoint  inspect checkpoint tools
      kafka       kafka consume tools
      migrate     migrate tools
      stat        show leader stat of scheduler