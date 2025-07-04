# Use an image with Python 3.13
FROM python:3.13-slim

# Install git and other system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user with home directory
RUN groupadd -r hypha_startup_services && useradd -r -g hypha_startup_services -m -d /home/hypha_startup_services hypha_startup_services

# Upgrade pip
RUN pip install --upgrade pip

# Set the working directory
WORKDIR /app/

# Copy the repository to the image
COPY . .

# Initialize and update submodules to ensure EB-1-Nodes-Technologies data is available
RUN git submodule update --init --recursive

# Create logs directory and set permissions for /app, /app/logs, and home directory
RUN mkdir -p /app/logs && \
    mkdir -p /home/hypha_startup_services/.mem0 && \
    chmod 777 -R /app /app/logs && \
    chmod 777 /home/hypha_startup_services && \
    chmod 777 /home/hypha_startup_services/.mem0 && \
    chown -R hypha_startup_services:hypha_startup_services /app /app/logs /home/hypha_startup_services

# Add /app to the list of safe directories for Git
RUN git config --global --add safe.directory /app

# Remove all files matching .gitignore patterns and .git directory
RUN git clean -fdX && rm -rf .git

# Install the required Python packages
RUN pip install -r requirements.txt
RUN pip install .

# Set environment variables for mem0 and other services
ENV HOME=/home/hypha_startup_services
ENV MEM0_DIR=/home/hypha_startup_services/.mem0

# Switch to the non-root user
USER hypha_startup_services

# Default entry point - starts all services (weaviate, mem0, bioimage) with --remote
# Can be overridden in Kubernetes deployment for single services:
# command: ["hypha-startup-services", "weaviate", "--remote"]
# command: ["hypha-startup-services", "mem0", "--remote"] 
# command: ["hypha-startup-services", "weaviate", "mem0", "--remote"]
CMD ["hypha-startup-services", "weaviate", "mem0", "bioimage", "--remote"]