FROM openjdk:11-jre-slim
COPY target/bet-stat-1.0-SNAPSHOT.jar ./bet-stat-1.0-SNAPSHOT.jar
ENTRYPOINT java -jar bet-stat-1.0-SNAPSHOT.jar

