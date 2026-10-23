FROM docker.io/library/alpine:3.22 AS plugin
ARG TARGETARCH
RUN apk add --no-cache curl unzip
RUN arch="${TARGETARCH:-$(uname -m)}" \
    && case "$arch" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; esac \
    && curl -fL --retry 3 "https://github.com/dolphindb/grafana-datasource-next/releases/download/3.0.700/dolphindb-datasource-next.v3.0.7.linux-${arch}.zip" -o /tmp/plugin.zip \
    && mkdir /plugin && unzip /tmp/plugin.zip -d /plugin \
    && find /plugin -type f -name 'gpx_*' -exec chmod +x {} \;

FROM docker.io/grafana/grafana:13.2.2
USER root
RUN apk add --no-cache jq
COPY --from=plugin --chown=grafana:root /plugin/ /opt/grafana-plugins/
COPY --chmod=755 entrypoint.sh /poc-entrypoint.sh
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins
USER 472
ENTRYPOINT ["/poc-entrypoint.sh"]
