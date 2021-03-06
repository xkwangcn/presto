FROM fedora:28 as build

RUN set -x && \
    INSTALL_PKGS="java-1.8.0-openjdk maven" \
    && yum clean all && rm -rf /var/cache/yum/* \
    && yum install -y \
        $INSTALL_PKGS  \
    && yum clean all \
    && rm -rf /var/cache/yum

RUN mkdir /build

COPY .git /build/.git
COPY presto-local-file /build/presto-local-file
COPY presto-resource-group-managers /build/presto-resource-group-managers
COPY presto-atop /build/presto-atop
COPY presto-memory /build/presto-memory
COPY presto-redshift /build/presto-redshift
COPY presto-benchmark-driver /build/presto-benchmark-driver
COPY presto-thrift-api /build/presto-thrift-api
COPY presto-kinesis /build/presto-kinesis
COPY presto-blackhole /build/presto-blackhole
COPY presto-verifier /build/presto-verifier
COPY presto-server-rpm /build/presto-server-rpm
COPY presto-orc /build/presto-orc
COPY presto-rcfile /build/presto-rcfile
COPY presto-base-jdbc /build/presto-base-jdbc
COPY presto-phoenix /build/presto-phoenix
COPY presto-geospatial-toolkit /build/presto-geospatial-toolkit
COPY presto-postgresql /build/presto-postgresql
COPY presto-sqlserver /build/presto-sqlserver
COPY presto-teradata-functions /build/presto-teradata-functions
COPY presto-ml /build/presto-ml
COPY presto-cassandra /build/presto-cassandra
COPY presto-server /build/presto-server
COPY presto-mysql /build/presto-mysql
COPY presto-parser /build/presto-parser
COPY presto-docs /build/presto-docs
COPY presto-kafka /build/presto-kafka
COPY presto-session-property-managers /build/presto-session-property-managers
COPY presto-iceberg /build/presto-iceberg
COPY presto-mongodb /build/presto-mongodb
COPY presto-record-decoder /build/presto-record-decoder
COPY presto-tpcds /build/presto-tpcds
COPY presto-plugin-toolkit /build/presto-plugin-toolkit
COPY presto-spi /build/presto-spi
COPY presto-prometheus /build/presto-prometheus
COPY presto-thrift-testing-server /build/presto-thrift-testing-server
COPY presto-cli /build/presto-cli
COPY presto-hive /build/presto-hive
COPY presto-matching /build/presto-matching
COPY presto-elasticsearch /build/presto-elasticsearch
COPY presto-accumulo /build/presto-accumulo
COPY presto-tests /build/presto-tests
COPY presto-thrift /build/presto-thrift
COPY presto-geospatial /build/presto-geospatial
COPY presto-jmx /build/presto-jmx
COPY presto-jdbc /build/presto-jdbc
COPY presto-tpch /build/presto-tpch
COPY presto-redis /build/presto-redis
COPY presto-array /build/presto-array
COPY presto-product-tests /build/presto-product-tests
COPY presto-client /build/presto-client
COPY presto-testing-server-launcher /build/presto-testing-server-launcher
COPY presto-parquet /build/presto-parquet
COPY presto-proxy /build/presto-proxy
COPY presto-hive-hadoop2 /build/presto-hive-hadoop2
COPY presto-benchto-benchmarks /build/presto-benchto-benchmarks
COPY presto-testing-docker /build/presto-testing-docker
COPY presto-memory-context /build/presto-memory-context
COPY presto-benchmark /build/presto-benchmark
COPY presto-example-http /build/presto-example-http
COPY presto-google-sheets /build/presto-google-sheets
COPY presto-kudu /build/presto-kudu
COPY presto-main /build/presto-main
COPY presto-raptor-legacy /build/presto-raptor-legacy
COPY presto-password-authenticators /build/presto-password-authenticators
COPY src /build/src
COPY pom.xml /build/pom.xml

# load Maven cache as its own layer
RUN cd /build && mvn --batch-mode --errors de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -DdownloadSources -DdownloadJavadoc -Dmaven.repo.local=.m2/repository

# build presto
RUN cd /build && mvn --batch-mode --errors -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -DskipTests -DfailIfNoTests=false -Dtest=false clean package -pl '!presto-testing-docker' -Dmaven.repo.local=.m2/repository
# Install prometheus-jmx agent
RUN mvn -B dependency:get -Dartifact=io.prometheus.jmx:jmx_prometheus_javaagent:0.3.1:jar -Ddest=/build/jmx_prometheus_javaagent.jar

FROM centos:7

# our copy of faq and jq
COPY faq.repo /etc/yum.repos.d/ecnahc515-faq-epel-7.repo

RUN set -x; \
    INSTALL_PKGS="java-1.8.0-openjdk java-1.8.0-openjdk-devel openssl less rsync faq" \
    && yum clean all \
    && rm -rf /var/cache/yum/* \
    && yum -y install epel-release \
    && yum install --setopt=skip_missing_names_on_install=False -y $INSTALL_PKGS \
    && yum clean all \
    && rm -rf /var/cache/yum

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

RUN mkdir -p /opt/presto

ENV PRESTO_VERSION 322
ENV PRESTO_HOME /opt/presto/presto-server
ENV PRESTO_CLI /opt/presto/presto-cli
# Note: podman was having difficulties evaluating the PRESTO_VERSION
# environment variables: https://github.com/containers/libpod/issues/4878
ARG PRESTO_VERSION=${PRESTO_VERSION}
ENV PROMETHEUS_JMX_EXPORTER /opt/jmx_exporter/jmx_exporter.jar
ENV TERM linux
ENV HOME /opt/presto
ENV JAVA_HOME=/etc/alternatives/jre

RUN mkdir -p $PRESTO_HOME

COPY --from=build /build/presto-server/target/presto-server-${PRESTO_VERSION} ${PRESTO_HOME}
COPY --from=build /build/presto-cli/target/presto-cli-${PRESTO_VERSION}-executable.jar ${PRESTO_CLI}
COPY --from=build /build/jmx_prometheus_javaagent.jar ${PROMETHEUS_JMX_EXPORTER}

# https://docs.oracle.com/javase/7/docs/technotes/guides/net/properties.html
# Java caches dns results forever, don't cache dns results forever:
RUN sed -i '/networkaddress.cache.ttl/d' $JAVA_HOME/lib/security/java.security
RUN sed -i '/networkaddress.cache.negative.ttl/d' $JAVA_HOME/lib/security/java.security
RUN echo 'networkaddress.cache.ttl=0' >> $JAVA_HOME/lib/security/java.security
RUN echo 'networkaddress.cache.negative.ttl=0' >> $JAVA_HOME/lib/security/java.security

RUN ln $PRESTO_CLI /usr/local/bin/presto-cli && \
    chmod 755 /usr/local/bin/presto-cli

RUN chown -R 1003:0 /opt/presto $JAVA_HOME/lib/security/cacerts && \
    chmod -R 774 $JAVA_HOME/lib/security/cacerts && \
    chmod -R 775 /opt/presto

USER 1003
EXPOSE 8080
WORKDIR $PRESTO_HOME

CMD ["tini", "--", "bin/launcher", "run"]

LABEL io.k8s.display-name="OpenShift Presto" \
      io.k8s.description="This is an image used by operator-metering to to install and run Presto." \
      io.openshift.tags="openshift" \
      maintainer="AOS Operator Metering <sd-operator-metering@redhat.com>"
