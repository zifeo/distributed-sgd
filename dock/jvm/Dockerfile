FROM frolvlad/alpine-oraclejdk8:slim

COPY build/distributed-sgd.jar /opt/

RUN echo "Europe/Zurich" > /etc/timezone

CMD ["java", "-jar", "/opt/distributed-sgd.jar"]
