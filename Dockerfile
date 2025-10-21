FROM eclipse-temurin:17-jre

# Set working directory
WORKDIR /app

# Copy the shadow jar (fat jar with all dependencies)
COPY build/libs/*-all.jar /app/cassandra-easy-stress.jar

# Create a non-root user to run the application (using a different UID to avoid conflicts)
RUN useradd -m -u 10001 stress && \
    chown -R stress:stress /app

USER stress

# Set the entrypoint
ENTRYPOINT ["java", "-jar", "/app/cassandra-easy-stress.jar"]

# Default command shows help
CMD ["--help"]

