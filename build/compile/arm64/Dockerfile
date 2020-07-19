FROM ubuntu:focal

MAINTAINER chubaofs



ENV TZ='Asia/Shanghai'
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN  apt-get -y  update
RUN  apt-get -y install vim wget curl build-essential cmake git golang gcc-9-aarch64-linux-gnu gcc-aarch64-linux-gnu g++-9-aarch64-linux-gnu g++-aarch64-linux-gnu

RUN  cd /usr/bin
WORKDIR /usr/bin
RUN  mkdir x86_64_bak
RUN   cp gcc x86_64_bak/
RUN   cp g++ x86_64_bak/
RUN   ln -s -f aarch64-linux-gnu-gcc gcc
RUN   ln -s -f aarch64-linux-gnu-g++ g++


WORKDIR /root
ADD buildcfs.sh /root
RUN  chmod a+x  /root/*.sh

#CMD /root/buildcfs.sh

#ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/thymeleaf-master.jar"]