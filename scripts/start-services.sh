#!/bin/bash

# Startup script for running both weaviate and mem0 services
# This script starts both services in parallel

set -e

# Default values
SERVER_URL="${SERVER_URL:-https://hypha.aicell.io}"
WEAVIATE_SERVICE_ID="${WEAVIATE_SERVICE_ID:-weaviate}"
MEM0_SERVICE_ID="${MEM0_SERVICE_ID:-mem0}"

echo "Starting Hypha startup services..."
echo "Server URL: $SERVER_URL"
echo "Weaviate Service ID: $WEAVIATE_SERVICE_ID"
echo "Mem0 Service ID: $MEM0_SERVICE_ID"

# Function to start weaviate service
start_weaviate_service() {
    echo "Starting Weaviate service..."
    hypha-startup-services weaviate remote \
        --server-url="$SERVER_URL" \
        --service-id="$WEAVIATE_SERVICE_ID" &
    WEAVIATE_PID=$!
    echo "Weaviate service started with PID: $WEAVIATE_PID"
}

# Function to start mem0 service
start_mem0_service() {
    echo "Starting Mem0 service..."
    hypha-startup-services mem0 remote \
        --server-url="$SERVER_URL" \
        --service-id="$MEM0_SERVICE_ID" &
    MEM0_PID=$!
    echo "Mem0 service started with PID: $MEM0_PID"
}

# Function to wait for weaviate server to be ready
wait_for_weaviate_server() {
    echo "Waiting for Weaviate server to be ready..."
    local retries=30
    local count=0
    
    while [ $count -lt $retries ]; do
        if curl -s http://localhost:8080/v1/.well-known/ready > /dev/null 2>&1; then
            echo "Weaviate server is ready!"
            return 0
        fi
        
        echo "Waiting for Weaviate server... (attempt $((count + 1))/$retries)"
        sleep 5
        count=$((count + 1))
    done
    
    echo "ERROR: Weaviate server did not become ready within expected time"
    return 1
}

# Function to handle shutdown
cleanup() {
    echo "Shutting down services..."
    if [ ! -z "$WEAVIATE_PID" ]; then
        kill $WEAVIATE_PID 2>/dev/null || true
        echo "Stopped Weaviate service"
    fi
    if [ ! -z "$MEM0_PID" ]; then
        kill $MEM0_PID 2>/dev/null || true
        echo "Stopped Mem0 service"
    fi
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT

# Wait for Weaviate server to be ready
wait_for_weaviate_server

# Start both services
start_weaviate_service
start_mem0_service

# Wait for both services to finish
echo "Both services started. Waiting for completion..."
wait $WEAVIATE_PID $MEM0_PID

echo "All services have stopped."
