FROM ubuntu:14.04
#FROM ubuntu:16.04

MAINTAINER cubefs



ENV TZ='Asia/Shanghai'
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


RUN  apt-get -y  update

RUN  apt-get -y install bash vim wget curl make  cmake3 git   software-properties-common python-software-properties

RUN add-apt-repository ppa:ubuntu-toolchain-r/aarch64
RUN  apt-get -y  update
# ubuntu16.04 using gcc-9-aarch64-linux-gnu，under Centos7.6 will fail：
#RUN apt-get -y install gcc-4.9-aarch64-linux-gnu gcc-aarch64-linux-gnu g++-4.9-aarch64-linux-gnu g++-aarch64-linux-gnu
RUN apt-get -y install  gcc-aarch64-linux-gnu  g++-aarch64-linux-gnu unzip
#go version 1.6,too low
RUN wget https://golang.google.cn/dl/go1.13.11.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.13.11.linux-amd64.tar.gz
RUN echo "export PATH=$PATH:/usr/local/go/bin" >>  /etc/profile
#RUN source /etc/profile
RUN  cd /usr/bin
WORKDIR /usr/bin
RUN  mkdir x86_64_bak
RUN   mv gcc x86_64_bak/
#RUN   mv g++ x86_64_bak/
RUN   ln -s -f aarch64-linux-gnu-gcc gcc
RUN   ln -s -f aarch64-linux-gnu-g++ g++


WORKDIR /root
ADD buildcfs.sh /root
RUN  chmod a+x  /root/*.sh
RUN echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bashrc
RUN apt-get -y install ssh

#CMD /root/buildcfs.sh

#ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/thymeleaf-master.jar"]


# docker build --rm --tag arm64_gcc4_golang1_13_ubuntu_14_04_cubefs ./build/compile/arm64/gcc4
# docker run  -v /root/arm64/cubefs:/root/cubefs arm64_gcc4_golang1_13_ubuntu_14_04_cubefs /root/buildcfs.sh
