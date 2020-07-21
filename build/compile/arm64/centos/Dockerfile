FROM centos:7.4.1708

MAINTAINER chubaofs



ENV TZ='Asia/Shanghai'
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN  yum -y  update
RUN yum -y groupinstall Development Tools
# using gcc-9-aarch64-linux-gnu，under Centos7.6 will fail：
# refrence  https://bbs.huaweicloud.com/forum/thread-21263-1-1.html
RUN  yum -y install bash vim wget curl make cmake git golang
#go version 1.6,too low
# RUN wget http://112.124.9.243/arm9net/mini2440/linux/arm-linux-gcc-4.4.3-20100728.tar.gz
#RUN tar -C  /usr/local/arm/ -xzf arm-linux-gcc-4.4.3-20100728.tar.gz

RUN  mkdir /usr/local/ARM-toolchain
RUN cd /usr/local/ARM-toolchain
RUN  wget https://releases.linaro.org/components/toolchain/binaries/latest-5/aarch64-linux-gnu/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu.tar.xz
RUN tar -xvf gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu.tar.xz

       

RUN echo "export PATH=$PATH:/usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin" >>  /etc/profile
#RUN source /etc/profile
RUN  cd /usr/bin
WORKDIR /usr/bin
RUN  mkdir x86_64_bak
RUN   cp gcc x86_64_bak/
#RUN   cp g++ x86_64_bak/
RUN cd /usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin
WORKDIR /usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin
RUN   ln -s -f aarch64-linux-gnu-gcc gcc
#RUN   ln -s -f aarch64-linux-gnu-g++ g++


WORKDIR /root
ADD buildcfs.sh /root
RUN  chmod a+x  /root/*.sh
RUN echo "export PATH=$PATH:/usr/local/ARM-toolchain/gcc-linaro-5.5.0-2017.10-x86_64_aarch64-linux-gnu/bin" >> ~/.bashrc

#CMD /root/buildcfs.sh
# docker build --rm --tag arm64_gcc4_golang1_13_centos7.4_chubaofs ./build/compile/arm64/centos
#  make dist-clean
# docker run  -v /root/arm64/chubaofs:/root/chubaofs rm64_gcc4_golang1_13_centos7.4_chubaofs /root/buildcfs.sh

#ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/thymeleaf-master.jar"]