FROM borenmon/bitnami-spark:3.5.1
USER root

ARG SPARK_JARS_DIR=/opt/bitnami/spark/jars

ARG POSTGRESQL_JDBC_VERSION=42.7.3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

RUN install_packages curl && \
    curl -fSL -o ${SPARK_JARS_DIR}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar \
      "https://jdbc.postgresql.org/download/postgresql-${POSTGRESQL_JDBC_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

USER 1001
