# Use an image with Python 3.11
FROM python:3.11-slim

# Install git and other system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -r hypha_startup_services && useradd -r -g hypha_startup_services hypha_startup_services

# Upgrade pip
RUN pip install --upgrade pip

# Set the working directory
WORKDIR /app/

# Copy the repository to the image
COPY . .

# Create logs directory and set permissions for /app and /app/logs
RUN mkdir -p /app/logs && \
    chmod 777 -R /app /app/logs && \
    chown -R hypha_startup_services:hypha_startup_services /app /app/logs

# Add /app to the list of safe directories for Git
RUN git config --global --add safe.directory /app

# Remove all files matching .gitignore patterns and .git directory
RUN git clean -fdX && rm -rf .git

# Install the required Python packages
RUN pip install -r requirements.txt
RUN pip install .

# Copy startup scripts and make them executable
COPY scripts/start-services.sh /app/start-services.sh
RUN chmod +x /app/start-services.sh

# Switch to the non-root user
USER hypha_startup_services

# Default entry point - can be overridden in Kubernetes deployment
# For running both services: command: ["/app/start-services.sh"]
# For single service: command: ["hypha-startup-services", "weaviate", "remote", ...]
ENTRYPOINT ["hypha-startup-services"]
CMD ["weaviate", "remote", "--server-url=https://hypha.aicell.io"]