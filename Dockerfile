FROM maven:3.9.3-amazoncorretto-20 as builder

WORKDIR /app

COPY pom.xml ./
RUN mvn dependency:resolve
RUN mvn dependency:tree

COPY src src/
RUN mvn clean install


FROM amazoncorretto:20

WORKDIR /app
COPY --from=builder /app/target/p2es-jar-with-dependencies.jar ./app.jar
COPY log4j2.yml ./

ENTRYPOINT ["java", "-XX:ParallelGCThreads=2", "-XX:+ExitOnOutOfMemoryError", "-XX:+PrintFlagsFinal", "-Xlog:async", "-XX:AsyncLogBufferSize=1M", "-Xlog:gc*=debug:stdout", "-Dpulsar.allocator.pooled=true", "-Dpulsar.allocator.out_of_memory_policy=ThrowException", "-Dpulsar.allocator.exit_on_oom=true", "-Dfile.encoding=UTF-8", "-Dlog4j.configurationFile=/app/log4j2.yml", "--add-opens=java.base/sun.net=ALL-UNNAMED", "--add-opens=java.management/sun.management=ALL-UNNAMED", "-jar", "app.jar"]
