FROM frolvlad/alpine-oraclejdk8:slim

ADD data /data
ADD build/dsgd.jar /opt/

RUN echo "Europe/Zurich" > /etc/timezone

CMD ["java", "-jar", "/opt/dsgd.jar"]
