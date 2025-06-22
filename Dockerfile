FROM openjdk:17-jdk-slim AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B -e -q -DskipTests package dependency:go-offline
COPY src src
RUN mvn -B -e -q -DskipTests package

FROM openjdk:17-jre-slim
WORKDIR /app
COPY --from=build /app/target/kid-migrator-1.0-SNAPSHOT.jar app.jar
COPY src/main/resources/application.properties application.properties
ENTRYPOINT ["java","-jar","app.jar"]
