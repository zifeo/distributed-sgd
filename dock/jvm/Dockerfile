FROM frolvlad/alpine-oraclejdk8:slim

ADD build/dsgd.jar /opt/
ADD data /data

RUN echo "Europe/Zurich" > /etc/timezone

CMD ["java", "-jar", "/opt/dsgd.jar"]
