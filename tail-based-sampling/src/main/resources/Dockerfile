FROM centos:centos7

ADD jdk8.tar.gz /usr/local/src
COPY tailbaseSampling-1.0-SNAPSHOT.jar /usr/local/src
WORKDIR /usr/local/src
COPY start.sh /usr/local/src
RUN chmod +x /usr/local/src/start.sh
ENTRYPOINT ["/bin/bash", "/usr/local/src/start.sh"]