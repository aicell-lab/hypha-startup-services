# Deploying Weaviate + Hypha Startup Services

This guide explains how to deploy both the Weaviate database server and the hypha startup services (weaviate and mem0 services) together.

## Overview

The deployment consists of:
1. **Weaviate Server**: The actual vector database (using the original Weaviate Helm chart)
2. **Weaviate Service**: Hypha service that exposes Weaviate functionality via the Hypha RPC framework
3. **Mem0 Service**: Hypha service that provides memory management capabilities

## Deployment Options

### Option 1: Using Extended Weaviate Values (Recommended)

Use the `values-weaviate-extended.yaml` file with the existing Weaviate Helm chart:

```bash
# Add the Weaviate Helm repository if not already added
helm repo add weaviate https://weaviate.github.io/weaviate-helm

# Deploy Weaviate with startup services using the extended values
helm install weaviate-with-services weaviate/weaviate \
  -f values-weaviate-extended.yaml \
  --namespace hypha-services \
  --create-namespace
```

This approach:
- Uses the official Weaviate Helm chart
- Adds startup services as additional deployments
- Maintains compatibility with Weaviate updates

### Option 2: Using Kubernetes Templates Directly

Apply the Kubernetes templates directly:

```bash
# First deploy Weaviate using its official chart with the original values
helm install weaviate weaviate/weaviate \
  -f /path/to/weaviate/values.yaml \
  --namespace hypha-services \
  --create-namespace

# Then deploy the startup services
kubectl apply -f k8s-templates/ -n hypha-services
```

### Option 3: Combined Helm Chart

Use the `values-combined.yaml` for a custom Helm chart that manages everything together (requires creating a custom chart).

## Configuration Details

### Image Configuration

Both services use the same Docker image but with different startup commands:

- **Weaviate Service**: `hypha-startup-services weaviate remote --server-url=https://hypha.aicell.io --service-id=weaviate-production`
- **Mem0 Service**: `hypha-startup-services mem0 remote --server-url=https://hypha.aicell.io --service-id=mem0-production`

### Environment Variables

Required environment variables:
- `HYPHA_TOKEN`: Token for connecting to the Hypha server (from secret `aria-agents-secrets`)
- `MEM0_API_KEY`: Optional API key for Mem0 service (from secret `aria-agents-secrets`)

### Security Context

All services run with:
- Non-root user (UID 1000)
- Dropped capabilities
- Read-only root filesystem (for Weaviate server)
- Security context profiles

### Resources

Default resource allocation per service:
- **Limits**: 500m CPU, 512Mi memory
- **Requests**: 100m CPU, 128Mi memory

## Service URLs

After deployment, the services will be available at:
- **Weaviate Service**: `https://hypha.aicell.io/aria-agents/services/weaviate-production`
- **Mem0 Service**: `https://hypha.aicell.io/aria-agents/services/mem0-production`

## Monitoring

The deployments include:
- **Liveness Probes**: Check that the processes are running
- **Readiness Probes**: Check that the services are ready to accept connections
- **Service Accounts**: For proper RBAC if needed

## Scaling

To scale the services:

```bash
# Scale weaviate startup service
kubectl scale deployment weaviate-startup-service --replicas=2 -n hypha-services

# Scale mem0 startup service  
kubectl scale deployment mem0-startup-service --replicas=2 -n hypha-services
```

## Troubleshooting

### Check pod status:
```bash
kubectl get pods -n hypha-services -l component=hypha-startup-services
```

### Check logs:
```bash
# Weaviate service logs
kubectl logs -l app=weaviate-startup-service -n hypha-services

# Mem0 service logs
kubectl logs -l app=mem0-startup-service -n hypha-services
```

### Test connectivity:
```bash
# Test if services are registered with Hypha
curl https://hypha.aicell.io/aria-agents/services/weaviate-production
curl https://hypha.aicell.io/aria-agents/services/mem0-production
```

## Updates

To update the startup services:

1. Update the image tag in the values file
2. Apply the changes:

```bash
helm upgrade weaviate-with-services weaviate/weaviate \
  -f values-weaviate-extended.yaml \
  --namespace hypha-services
```

Or if using kubectl:

```bash
kubectl set image deployment/weaviate-startup-service weaviate-service=ghcr.io/aicell-lab/hypha-startup-services:new-tag -n hypha-services
kubectl set image deployment/mem0-startup-service mem0-service=ghcr.io/aicell-lab/hypha-startup-services:new-tag -n hypha-services
```
