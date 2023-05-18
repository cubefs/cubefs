FROM centos:7.5.1804

MAINTAINER cubefs



ENV TZ='Asia/Shanghai'
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone




RUN  yum -y  update
RUN yum -y groupinstall Development Tools
# using gcc-9-aarch64-linux-gnu，under Centos7.6 will fail：
# refrence  https://bbs.huaweicloud.com/forum/thread-21263-1-1.html
RUN  yum -y install bash vim wget curl make cmake  git golang

#go version 1.6,too low
# RUN wget http://112.124.9.243/arm9net/mini2440/linux/arm-linux-gcc-4.4.3-20100728.tar.gz
#RUN tar -C  /usr/local/arm/ -xzf arm-linux-gcc-4.4.3-20100728.tar.gz

RUN  mkdir /usr/local/ARM-toolchain
RUN cd /usr/local/ARM-toolchain
WORKDIR  /usr/local/ARM-toolchain
RUN  wget https://releases.linaro.org/components/toolchain/binaries/latest-5/aarch64-linux-gnu/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu.tar.xz
#RUN wget http://10.2.7.8/www/CentOS/7/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu.tar.xz
RUN xz -d gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu.tar.xz 
RUN tar -xvf gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu.tar 

RUN wget https://github.com/Kitware/CMake/releases/download/v3.18.0/cmake-3.18.0-Linux-x86_64.tar.gz
#RUN wget http://10.2.7.8/www/CentOS/7/cmake-3.18.0-Linux-x86_64.tar.gz
RUN tar zxf cmake-3.18.0-Linux-x86_64.tar.gz

RUN wget https://golang.google.cn/dl/go1.13.11.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.13.11.linux-amd64.tar.gz


       

RUN echo "export PATH=/usr/local/go/bin:/usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin:/usr/local/ARM-toolchain/cmake-3.18.0-Linux-x86_64/bin:$PATH" >>  /etc/profile
#RUN source /etc/profile
RUN  cd /usr/bin
WORKDIR /usr/bin
RUN  mkdir x86_64_bak
RUN   mv gcc x86_64_bak/
RUN   mv g++ x86_64_bak/
RUN   ln -s -f /usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-gcc gcc
#RUN   ln -s -f aarch64-linux-gnu-g++ g++
# must have g++
RUN ln -s -f /usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin/aarch64-linux-gnu-gcc g++



WORKDIR /root
ADD buildcfs.sh /root
RUN  chmod a+x  /root/*.sh
RUN echo "export PATH=/usr/local/go/bin:/usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin:/usr/local/ARM-toolchain/cmake-3.18.0-Linux-x86_64/bin:$PATH" >> ~/.bashrc



#CMD /root/buildcfs.sh
# docker build --rm --tag arm64_gcc4_golang1_13_centos7_5_cubefs ./build/compile/arm64/centos
#  make dist-clean
# docker run  -v /root/arm64/cubefs:/root/cubefs arm64_gcc4_golang1_13_centos7_5_cubefs /root/buildcfs.sh

#ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/thymeleaf-master.jar"]