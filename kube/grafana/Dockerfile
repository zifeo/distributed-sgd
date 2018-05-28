FROM grafana/grafana:latest

ENV GF_SECURITY_ADMIN_USER root
ENV GF_SECURITY_ADMIN_PASSWORD secret
ENV GF_USERS_ALLOW_SIGN_UP false

ADD influxdb.yaml /etc/grafana/provisioning/datasources/influxdb.yaml
