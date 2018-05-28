FROM frolvlad/alpine-oraclejdk8:slim

ADD build/dsgd.jar /opt/

RUN echo "Europe/Zurich" > /etc/timezone

CMD ["java", "-jar", "/opt/dsgd.jar", "-Xmx16G", "-Xms10G"]
