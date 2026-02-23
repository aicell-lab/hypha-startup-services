# Weaviate Hypha App

This application provides the Weaviate service interface for the Hypha RPC framework. It wraps a Weaviate vector database instance and exposes it as a Hypha service.

## Prerequisites

- A running Weaviate instance
- Hypha server access

## Configuration

The application expects the following environment variables to be set in the runtime environment:

- `WEAVIATE_HTTP_HOST`: Weaviate HTTP host (default: `hypha-weaviate.scilifelab-2-dev.sys.kth.se`)
- `WEAVIATE_HTTP_PORT`: Weaviate HTTP port (default: `443`)
- `WEAVIATE_HTTP_SECURE`: Use HTTPS (default: `true`)
- `WEAVIATE_GRPC_HOST`: Weaviate gRPC host (default: `hypha-weaviate-grpc.scilifelab-2-dev.sys.kth.se`)
- `WEAVIATE_GRPC_PORT`: Weaviate gRPC port (default: `443`)
- `WEAVIATE_GRPC_SECURE`: Use secure gRPC (default: `true`)
- `WEAVIATE_SERVICE_ID`: The service ID to register (default: `weaviate`)

## Development


To run the app locally:

1. Run the app loader (simulating Hypha environment):

  ```bash
  # Make sure dependencies are installed
  pip install -r ../requirements.txt

  # Run app
  python app.py
  ```

  *Note: Running `python app.py` directly won't register the service with a remote Hypha server unless you wrap it with a Hypha client connection script. See `scripts/deploy_weaviate_app.py` for deployment.*

## Deployment

The app is deployed using GitHub Actions workflow or the deployment script:

```bash
python scripts/deploy_weaviate_app.py \
  --server-url <HYPHA_SERVER_URL> \
  --token <HYPHA_TOKEN> \
  --app-id weaviate \
  --source weaviate-app/app.py \
  --manifest weaviate-app/manifest.yaml
```
