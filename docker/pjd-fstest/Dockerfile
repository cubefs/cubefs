# ============================================================================
# pjd-fstest — POSIX filesystem compliance test suite
# https://github.com/pjd/pjdfstest (forked from FreeBSD fstest)
#
# This image is consumed by the dashboard's "POSIX Compliance" feature, which
# launches a one-shot K8s Job that mounts a target CubeFS volume into
# /mnt/target and runs `prove` over the pjd test files. The result TAP stream
# is written to stdout so the dashboard backend can pull it from pod logs.
# ============================================================================
FROM hub.shiyak-office.com/storage/ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential perl libacl1-dev wget make ca-certificates \
        libtest-harness-perl libtap-harness-archive-perl \
        attr acl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN wget -q http://download.ceph.com/qa/pjd-fstest-20090130-RC-aclfixes.tgz && \
    tar xzf pjd-fstest-20090130-RC-aclfixes.tgz && \
    mv pjd-fstest-20090130-RC pjd-fstest && \
    rm pjd-fstest-20090130-RC-aclfixes.tgz && \
    cd pjd-fstest && make

ENV PJD_TESTS=/opt/pjd-fstest/tests

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
