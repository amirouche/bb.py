# Bonafide Storage Primitives

Bonafide is a storage layer for bb.py that implements an ordered
key-value store using SQLite. It provides thread-safe access to the
database through a connection pool, ensuring efficient concurrency
while respecting SQLite's single-writer constraint. This document
tracks the plan and todos for building bonafide.

## Plan

- [x] **Thread Pool**: Create a thread pool to manage database connections.
- [x] **Thread-Specific Connections**: Ensure each thread has its own connection to the database.
- [x] **Locking Mechanism**: Implement a lock to manage write operations safely.
- [x] **Connection Pool Initialization**: Initialize the connection pool with a configurable size.
- [x] **Query Execution**: Implement functions to execute queries within transactions.
- [ ] **Error Handling**: Add robust error handling for database operations.
- [x] **Testing**: Write tests to verify thread safety and concurrency.
- [ ] **Documentation**: Document the API and usage examples.
