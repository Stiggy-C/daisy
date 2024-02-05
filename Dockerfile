FROM docker.io/bitnami/spark:3.3.3

USER root

RUN apt-get update && apt-get install liblz4-1 && apt-get install liblzf1 && apt-get install libzstd1

# The following is for debug purposes:
RUN apt-get -y install telnet

USER 1001

# Add JDBC drivers
ADD --chown=1001 https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar /opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar /opt/bitnami/spark/jars/postgresql-42.6.0.jar

# Add Apache Spark dependencies
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.1.2/kafka-clients-3.1.2.jar /opt/bitnami/spark/jars/kafka-clients-3.1.2.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.3/spark-sql-kafka-0-10_2.12-3.3.3.jar /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.3.3.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.3/spark-token-provider-kafka-0-10_2.12-3.3.3.jar /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.3.3.jar

# Add HikariCP
ADD --chown=1001 https://repo1.maven.org/maven2/com/zaxxer/HikariCP/4.0.3/HikariCP-4.0.3.jar /opt/bitnami/spark/jars/HikariCP-4.0.3.jar

# Add Spring Framework dependencies
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-core/5.3.29/spring-core-5.3.29.jar /opt/bitnami/spark/jars/spring-core-5.3.29.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-beans/5.3.29/spring-beans-5.3.29.jar /opt/bitnami/spark/jars/spring-beans-5.3.29.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-jcl/5.3.29/spring-jcl-5.3.29.jar /opt/bitnami/spark/jars/spring-jcl-5.3.29.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-jdbc/5.3.29/spring-jdbc-5.3.29.jar /opt/bitnami/spark/jars/spring-jdbc-5.3.29.jar
ADD --chown=1001 https://repo1.maven.org/maven2/org/springframework/spring-tx/5.3.29/spring-tx-5.3.29.jar /opt/bitnami/spark/jars/spring-tx-5.3.29.jar

RUN chmod 664 /opt/bitnami/spark/jars/commons-pool2-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/HikariCP-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/kafka-clients-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/mysql-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/postgresql-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/spark-sql-kafka-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/spark-token-provider-kafka-*.jar
RUN chmod 664 /opt/bitnami/spark/jars/spring-*.jar

WORKDIR /opt/bitnami/spark
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]

CMD /opt/bitnami/spark/sbin/start-master.sh | /opt/bitnami/spark/sbin/start-worker.sh spark://$(hostname):7077 | /opt/bitnami/spark/sbin/start-thriftserver.sh