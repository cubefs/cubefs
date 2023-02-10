===================================================
Fault Domain Configuration and Management Commands
===================================================

---------------------------
1. Upgrade and configuration
---------------------------

In the cross zone scenario, the reliability need to be improved. Compared with the 2.5 version before, the number of copysets in probability can be reduced. The key point is to use fault domains to group nodesets between multiple zones.

**Reliable papers**

.. code-block:: bash

  https://www.usenix.org/conference/atc13/technical-sessions/presentation/cidon

**Chinese can refer to**

.. code-block:: bash

  https://zhuanlan.zhihu.com/p/28417779



1) Cluster level configuration
================
**fault domain**
The default is to support the original configuration. A special configuration is required to enable the fault domain. 
Increase the cluster-level configuration and whether to support the fault domain.

.. code-block:: bash

  FaultDomain  bool  // false default

Otherwise, it is impossible to distinguish whether the newly added zone is a fault domain zone or belongs to the original cross_zone

**Zone count to build domain**

.. code-block:: bash

  faultDomainGrpBatchCnt，default count:3，can also set 2 or 1

If zone is unavaliable caused by network partition interruption，create nodeset group according to usable zone

Set “faultDomainBuildAsPossible” true, default is false

The distribution of nodesets under the number of different faultDomainGrpBatchCnt


.. code-block:: bash


      3 zone（1 nodeset per zone）

      2 zone（2 nodesets zone，1 nodeset zone，Take the size of the space as the weight, and build 2 nodeset with the larger space remaining）

      1 zone（3 nodeset in 1 zone）





2) Volume configuration
================
Reserve：

.. code-block:: bash

    crossZone        bool 

Add：

.. code-block:: bash

    default_priority bool
    
    is true to take effect, and the original zone is preferentially selected instead of being allocated from the fault domain.



3) Fault domain zone identification
================
1. Configure the current master as crosszone, restart the master, and then add a new zone

2. Restart in order to persist the current zone as a non-fault domain zone (the persistence layer does not have this information, and the default current zone should all be persisted as the old zone (default zone))

3. After restarting, loading, and adding a new zone later, the default is the new fault domain zone; and persistent;


4) Configuration summary
================

=========================  =========================  ======================  ===================================================================================
  Cluster:faultDomain           Vol:crossZone           Vol:normalZonesFirst     Rules for volume to use domain
=========================  =========================  ======================  ===================================================================================
N                                  N/A                        N/A                     Do not support domain
Y                                  N                          N/A               Write origin resources first before fault domain until origin reach threshold
Y                                  Y                          N                       Write fault domain only
Y                                  Y                          Y                 Write origin resources first before fault domain until origin reach threshold
=========================  =========================  ======================  ===================================================================================

---------------------------
2. Note
---------------------------

1) After the fault domain is enabled, all devices in the new zone will join the fault domain

2) The created volume will preferentially select the resources of the original zone

3) Need add configuration items to use domain resources when creating a new volume according to the table below. By default, the original zone resources are used first if it’s avaliable

---------------------------
3. management commands
---------------------------



Create volumes that use fault domains
=============

.. code-block:: bash

      curl "http://192.168.0.11:17010/admin/createVol?name=volDomain&capacity=1000&owner=cfs&crossZone=true&normalZonesFirst=true"


.. csv-table:: param list
   :header: "param", "type", "depict"
   
   "crossZone", "string", "Whether to cross zone"
   "normalZonesFirst", "Non-fault domain first", ""



View fault domain usage
=============
.. code-block:: bash

      curl -v  "http://192.168.0.11:17010/admin/getDomainInfo"


Update fault domain data usage cap
=============
.. code-block:: bash

      curl "http://192.168.0.11:17010/admin/updateDomainDataRatio?ratio=0.7"
      
      
View non-fault domain data usage caps
=============
.. code-block:: bash

      curl "http://192.168.0.11:17010/admin/updateZoneExcludeRatio?ratio=0.7"
