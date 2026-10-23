FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /repo/services/control-plane
COPY services/control-plane/pom.xml .
RUN mvn -B dependency:go-offline
COPY services/control-plane/src src
COPY packages/contracts /repo/packages/contracts
RUN mvn -B package

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S nexus && adduser -S nexus -G nexus
WORKDIR /app
COPY --from=build /repo/services/control-plane/target/control-plane-*.jar app.jar
USER nexus
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
