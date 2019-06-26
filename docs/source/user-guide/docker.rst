Docker
-----------------------

Under the docker directory, a helper tool called run_docker.sh is provided to run ChubaoFS with docker-compose.

To start a minimal ChubaoFS cluster from scratch, note that **/data/disk** is arbitrary, and make sure there are at least 30G available space.

.. code-block:: bash

    $ docker/run_docker.sh -r -d /data/disk

If client starts successfully, use `mount` command in client docker shell to check mount status:

.. code-block:: bash

    $ mount | grep chubaofs

Open http://127.0.0.1:3000 in browser, login with `admin/123456` to view grafana monitor metrics.

Or run server and client seperately by following commands:


.. code-block:: bash

    $ docker/run_docker.sh -b
    $ docker/run_docker.sh -s -d /data/disk
    $ docker/run_docker.sh -c
    $ docker/run_docker.sh -m

For more usage:

.. code-block:: bash

    $ docker/run_docker.sh -h

Prometheus and Grafana confg can be found in `docker/monitor` directory.
