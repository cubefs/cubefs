Scheduler
===============


Statistics
--------
.. code-block:: bash

   curl http://127.0.0.1:9800/stats

Response

.. code-block:: json

   {
	"repair": {
		"switch": "Enable",
		"repairing_disk_id": 0,
		"total_tasks_cnt": 0,
		"repaired_tasks_cnt": 0,
		"preparing_cnt": 0,
		"worker_doing_cnt": 0,
		"finishing_cnt": 0,
		"stats_per_min": {
			"finished_cnt": "[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
			"shard_cnt": "[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
			"data_amount_byte": "[0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B]"
		}
	},
	"balance": {
		"switch": "Enable",
		"preparing_cnt": 0,
		"worker_doing_cnt": 0,
		"finishing_cnt": 0,
		"stats_per_min": {
			"finished_cnt": "[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
			"shard_cnt": "[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]",
			"data_amount_byte": "[0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B 0.000B]"
		}
	},
	"inspect": {
		"switch": "Enable",
		"finished_per_min": "[437 439 444 433 437 443 436 441 433 437 446 434 440 434 436 449 432 439 435 195]",
		"time_out_per_min": "[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]"
	}
   }

