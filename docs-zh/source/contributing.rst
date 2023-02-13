对CubeFS作出贡献
========================

报告问题
-----------

请合理的使用关键字搜索 `查找问题 <https://github.com/cubeFS/cubefs/search?q=&type=Issues&utf8=%E2%9C%93>`_ ，以确保问题没有提交。然后通过 `打开问题 <https://github.com/cubeFS/cubefs/issues>`_ 提交问题描述和复现方法。

补丁指南
----------------

为了使补丁更可能被接受，您可能需要注意以下几点:

- 提交请求前需要先进行 `文件系统压力测试 <https://github.com/linux-test-project/ltp/blob/master/runtest/fs>`_

.. code-block:: bash

  runltp -f fs -d [MOUNTPOINT]

- 提供描述错误修复或新功能的提交消息
- `DCO <https://github.com/apps/dco>`_ 是必须的, 请在commit中添加 `Signed-off-by`

感谢
-------

文中部分内容转载自 `CoreDNS <https://github.com/coredns/coredns/blob/master/CONTRIBUTING.md>`_ 和 `Fluentd <https://github.com/fluent/fluentd/blob/master/CONTRIBUTING.md>`_
