# Weaviate Service Tests

This directory contains tests for the Weaviate service functionality.

## Test Structure

The tests are organized by functionality to keep files manageable and focused:

- `test_weaviate_admin.py` - Admin-specific functionality tests
- `test_weaviate_application.py` - Application management tests
- `test_weaviate_collection.py` - Collection management tests
- `test_weaviate_cross_tenant.py` - Cross-tenant functionality tests
- `test_weaviate_data.py` - Data operations tests (insert/update/delete)
- `test_weaviate_multiuser.py` - Multi-user functionality tests
- `test_weaviate_query.py` - Query functionality tests
- `test_weaviate_tenant.py` - Tenant-specific functionality tests

## Test Utilities

Common test utilities and constants are defined in `weaviate_test_utils.py`.

## Running Tests

Run all tests:
```
python -m pytest tests/
```

Run specific test file:
```
python -m pytest tests/test_weaviate_collection.py
```

Run specific test function:
```
python -m pytest tests/test_weaviate_collection.py::test_create_collection
```
