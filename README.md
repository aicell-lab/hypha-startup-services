# Hypha Startup Services

A collection of specialized AI-powered services for data management and semantic search, built on top of the [Hypha](https://hypha.aicell.io) framework. These services provide both general-purpose and bioimage-specific capabilities for vector storage, memory management, and intelligent data queries.

## 🚀 Services Overview

This repository contains four main services:

### Core Services
- **[Weaviate Service](./hypha_startup_services/weaviate_service/README.md)** - General-purpose vector database service with multi-tenant support
- **[Mem0 Service](./hypha_startup_services/mem0_service/README.md)** - AI memory management service with persistent storage

### BioImage-Specific Services
- **[Weaviate BioImage Service](./hypha_startup_services/weaviate_bioimage_service/README.md)** - Specialized Weaviate service for bioimage data
- **[Mem0 BioImage Service](./hypha_startup_services/mem0_bioimage_service/README.md)** - Specialized Mem0 service for bioimage data with semantic search

The bioimage services are specialized applications of the core services, demonstrating domain-specific implementations for EuroBioImaging node and technology data management.

## 📦 Installation

### Prerequisites
- Python 3.13
- Conda (recommended) or pip

### Setup Instructions

1. **Create and activate conda environment:**
   ```bash
   conda create -n hypha_startup_services python=3.13
   conda activate hypha_startup_services
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements_test.txt
   ```

3. **Install in development mode:**
   ```bash
   pip install -e .
   ```

## 🏃‍♂️ Quick Start

### Starting Services Locally

```bash
# Start Weaviate service
python -m hypha_startup_services weaviate local

# Start Mem0 service  
python -m hypha_startup_services mem0 local

# Start BioImage services
python -m hypha_startup_services weaviate-bioimage local
python -m hypha_startup_services mem0-bioimage local
```

### Connecting to Remote Hypha Server

```bash
# Connect services to remote Hypha server
python -m hypha_startup_services weaviate remote --server-url https://hypha.aicell.io
python -m hypha_startup_services mem0 remote --server-url https://hypha.aicell.io
```

## 🧪 Testing

⚠️ **Testing Prerequisites**: Before running tests, start the services in remote mode to ensure proper connectivity and functionality.

Run the full test suite:
```bash
pytest tests/ -v
```

Run tests for specific services:
```bash
# Test Weaviate service
pytest tests/weaviate_service/ -v

# Test Mem0 service
pytest tests/mem0_service/ -v

# Test BioImage services
pytest tests/weaviate_bioimage_service/ -v
pytest tests/mem0_bioimage_service/ -v
```

## 🖥️ Command Line Interface

The CLI provides flexible options for running services locally or connecting to remote Hypha servers:

```bash
python -m hypha_startup_services --help
```

**Output:**
```
usage: python -m hypha_startup_services [-h] {weaviate,mem0,weaviate-bioimage,mem0-bioimage} {local,remote} ...

Start Hypha services for data management and AI applications

positional arguments:
  {weaviate,mem0,weaviate-bioimage,mem0-bioimage}
                        Service to start
  {local,remote}        Mode to run the service in

options:
  -h, --help            show this help message and exit

Local mode options:
  --port PORT           Port to run the local server on (default: 9527)
  --service-id SERVICE_ID
                        Custom service ID (default: based on service type)

Remote mode options:
  --server-url SERVER_URL
                        URL of the remote Hypha server (default: https://hypha.aicell.io)
  --service-id SERVICE_ID
                        Custom service ID (default: based on service type)
```

### Usage Examples

**Local Development:**
```bash
# Start with default settings
python -m hypha_startup_services weaviate local

# Custom port and service ID
python -m hypha_startup_services weaviate local --port 8080 --service-id my-weaviate

# Start bioimage services
python -m hypha_startup_services weaviate-bioimage local
python -m hypha_startup_services mem0-bioimage local
```

**Remote Deployment:**
```bash
# Connect to default Hypha server
python -m hypha_startup_services mem0 remote

# Connect to custom server
python -m hypha_startup_services mem0 remote --server-url https://my-hypha-server.com

# Custom service ID for remote deployment
python -m hypha_startup_services weaviate remote --service-id production-weaviate
```

## 🏗️ Architecture

### Service Structure
```
hypha_startup_services/
├── common/              # Shared utilities and components
├── weaviate_service/    # Core vector database service
├── mem0_service/        # Core memory management service
├── weaviate_bioimage_service/  # BioImage-specific Weaviate
└── mem0_bioimage_service/      # BioImage-specific Mem0
```

### Key Features
- **Multi-tenant Support**: Isolated data spaces per workspace
- **Permission Management**: Fine-grained access controls
- **Vector Search**: Semantic similarity and hybrid search
- **Memory Management**: Persistent AI memory with context
- **Schema Flexibility**: Dynamic collection schemas
- **Bioimage Specialization**: Domain-specific data models

## 📚 Service Documentation

Each service provides comprehensive documentation:

- **[Weaviate Service](./hypha_startup_services/weaviate_service/README.md)** - Collections, queries, data operations
- **[Mem0 Service](./hypha_startup_services/mem0_service/README.md)** - Memory agents, runs, conversation context
- **[Weaviate BioImage Service](./hypha_startup_services/weaviate_bioimage_service/README.md)** - BioImage vector search and retrieval
- **[Mem0 BioImage Service](./hypha_startup_services/mem0_bioimage_service/README.md)** - BioImage memory and semantic search

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: Service-specific READMEs in each service directory
- **Issues**: [GitHub Issues](https://github.com/aicell-lab/hypha-startup-services/issues)
- **Hypha Framework**: [https://hypha.aicell.io](https://hypha.aicell.io)

## 🔗 Related Projects

- [Hypha](https://github.com/amun-ai/hypha) - The underlying RPC framework
- [Weaviate](https://weaviate.io/) - Vector database technology
- [Mem0](https://mem0.ai/) - AI memory platform
- [EuroBioImaging](https://www.eurobioimaging.eu/) - European bioimage infrastructure
