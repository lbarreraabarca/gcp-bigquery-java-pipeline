FROM maven:3.8-jdk-11 as builder

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn package -DskipTests | grep BUILD

FROM adoptopenjdk/openjdk11:alpine-jre

COPY --from=builder /app/target/*.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]

