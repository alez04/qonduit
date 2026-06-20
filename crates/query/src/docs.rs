//! OpenAPI spec and API documentation UI.
//!
//! Serves the OpenAPI JSON at `/openapi.json`, an interactive Scalar API
//! reference at `/docs`, and a comprehensive static documentation page at
//! `/guide`.

use axum::{http::StatusCode, response::IntoResponse, response::Response, routing::get, Json, Router};
use serde_json::json;
use std::sync::Arc;

use crate::AppState;

/// The OpenAPI 3.1 spec for Qonduit REST API.
pub fn openapi_spec() -> serde_json::Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Qonduit API",
            "description": "Qubic blockchain indexer and RPC server. Provides real-time and historical data from the Qubic network including ticks, transactions, entities, computors, assets, and smart contract data.\n\n## Data Freshness\nIndexed data may lag behind the live network by a few seconds. Use the `/system-info` endpoint to check `ticks_behind` for current lag.\n\n## Rate Limiting\nNo rate limiting is currently enforced, but this may change in production.\n\n## Authentication\nCurrently no authentication is required.",
            "version": env!("CARGO_PKG_VERSION")
        },
        "servers": [
            { "url": "/", "description": "Current server" }
        ],
        "paths": {
            "/health": {
                "get": {
                    "operationId": "health",
                    "summary": "Health check",
                    "description": "Returns the health status of the Qonduit server. Use this endpoint to verify that the server is running and responsive. The response includes the server version and uptime.\n\nThis endpoint is useful for load balancer health checks and monitoring.",
                    "tags": ["system"],
                    "responses": {
                        "200": {
                            "description": "Server is healthy",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/HealthResponse" },
                                    "example": {
                                        "status": "ok",
                                        "version": "0.1.0",
                                        "uptime_seconds": 3600
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/system-info": {
                "get": {
                    "operationId": "systemInfo",
                    "summary": "System and pipeline status",
                    "description": "Returns detailed information about the Qonduit system state including the ingestion pipeline status, indexing progress, and connection state to the Qubic node.\n\nUse `ticks_behind` to determine how far the indexed data is from the live network. A `pipeline_status` of `live` means the indexer is keeping up with the network in real time. `catching_up` indicates the server is processing a backlog of ticks. `query_only` means the REST API is available but no live data is being ingested.",
                    "tags": ["system"],
                    "responses": {
                        "200": {
                            "description": "Pipeline and system status",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SystemInfoResponse" },
                                    "example": {
                                        "pipeline_status": "live",
                                        "ingestion_connected": true,
                                        "node_tick": 60350000,
                                        "node_epoch": 218,
                                        "indexed_tick": 60349995,
                                        "indexed_epoch": 218,
                                        "ticks_behind": 5,
                                        "ticks_indexed": 12345678,
                                        "txs_indexed": 4567890,
                                        "entities_indexed": 234567,
                                        "uptime_seconds": 86400,
                                        "version": "0.1.0"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/v1/tick": {
                "get": {
                    "operationId": "getCurrentTick",
                    "summary": "Get latest tick data",
                    "description": "Returns the most recently indexed tick from the Qubic network. This is the latest tick that the indexer has processed and stored. The tick data includes epoch, timestamp, computor index, and other consensus information.\n\nNote: This endpoint returns the latest *indexed* tick, which may be a few seconds behind the live network tick.",
                    "tags": ["ticks"],
                    "responses": {
                        "200": {
                            "description": "Latest tick data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/TickData" },
                                    "example": {
                                        "epoch": 218,
                                        "tick": 60349995,
                                        "timestamp": 1718859600,
                                        "computor_index": 42
                                    }
                                }
                            }
                        },
                        "404": { "description": "No tick data available yet (indexer may still be starting up)" }
                    }
                }
            },
            "/v1/tick/{tick}": {
                "get": {
                    "operationId": "getTick",
                    "summary": "Get tick data by number",
                    "description": "Retrieves the full tick data for a specific tick number. Returns the epoch, timestamp, computor signatures, and other consensus data. Returns 404 if the tick hasn't been indexed yet.\n\nTick numbers are sequential unsigned 32-bit integers starting from 0. The current tick can be determined using the `/system-info` endpoint.",
                    "tags": ["ticks"],
                    "parameters": [
                        {
                            "name": "tick",
                            "in": "path",
                            "required": true,
                            "description": "The tick number to retrieve (0 to current indexed tick)",
                            "schema": { "type": "integer", "format": "uint32", "minimum": 0 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Tick data found and returned",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/TickData" },
                                    "example": {
                                        "epoch": 218,
                                        "tick": 60349088,
                                        "timestamp": 1718859600,
                                        "computor_index": 42
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid tick number" },
                        "404": { "description": "Tick not found in index" }
                    }
                }
            },
            "/v1/tick/{tick}/tx": {
                "get": {
                    "operationId": "getTickTransactions",
                    "summary": "Get transactions in a tick",
                    "description": "Returns all transactions that were included in the specified tick. Transactions are returned as an array and include sender/recipient identities, amounts, and other metadata.\n\nThis endpoint is useful for inspecting the contents of a specific block (tick) in the Qubic chain. If the tick has no transactions, an empty array is returned.",
                    "tags": ["transactions"],
                    "parameters": [
                        {
                            "name": "tick",
                            "in": "path",
                            "required": true,
                            "description": "The tick number whose transactions to retrieve",
                            "schema": { "type": "integer", "format": "uint32", "minimum": 0 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of transactions in the tick",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Transaction" }
                                    },
                                    "example": [
                                        {
                                            "hash": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                                            "source_hex": "4a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60616263646566676869",
                                            "source_identity": "AAAAABBBBBCCCCCDDDDD",
                                            "destination_hex": "6a6b6c6d6e6f707172737475767778797a7b7c7d7e7f80818283848586878889",
                                            "destination_identity": "EEEEFFFFFGGGGGHHHHH",
                                            "amount": 1000000,
                                            "tick": 60349088,
                                            "input_type": 1,
                                            "input_size": 0
                                        }
                                    ]
                                }
                            }
                        },
                        "400": { "description": "Invalid tick number" },
                        "404": { "description": "Tick not found" }
                    }
                }
            },
            "/v1/tx/{hash}": {
                "get": {
                    "operationId": "getTransaction",
                    "summary": "Get transaction by hash",
                    "description": "Retrieves a specific transaction by its 64-character hexadecimal hash. The hash uniquely identifies a transaction on the Qubic network and is computed from the transaction payload.\n\nThis endpoint returns full transaction details including source and destination identities, amounts, the tick it was included in, and input type information.",
                    "tags": ["transactions"],
                    "parameters": [
                        {
                            "name": "hash",
                            "in": "path",
                            "required": true,
                            "description": "The 64-character hexadecimal transaction hash",
                            "schema": { "type": "string", "minLength": 64, "maxLength": 64, "pattern": "^[0-9a-fA-F]{64}$" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Transaction found and returned",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Transaction" },
                                    "example": {
                                        "hash": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                                        "source_hex": "4a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60616263646566676869",
                                        "source_identity": "AAAAABBBBBCCCCCDDDDD",
                                        "destination_hex": "6a6b6c6d6e6f707172737475767778797a7b7c7d7e7f80818283848586878889",
                                        "destination_identity": "EEEEFFFFFGGGGGHHHHH",
                                        "amount": 1000000,
                                        "tick": 60349088,
                                        "input_type": 1,
                                        "input_size": 0
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid transaction hash: must be exactly 64 hex characters (0-9, a-f, A-F)" },
                        "404": { "description": "Transaction not found in index" }
                    }
                }
            },
            "/v1/entity/{id}": {
                "get": {
                    "operationId": "getEntity",
                    "summary": "Get entity (account) data",
                    "description": "Retrieves account data for a specific Qubic entity identified by its Base26-encoded identity string. Returns the identity and cumulative incoming/outgoing transaction totals.\n\nEntities represent accounts on the Qubic network. Each entity has a unique identity that can be used to look up its transaction history and balances.",
                    "tags": ["entities"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "description": "Base26-encoded entity identity (e.g. 'AAAAABBBBBCCCCCDDDDD')",
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Entity data found and returned",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Entity" },
                                    "example": {
                                        "identity": "AAAAABBBBBCCCCCDDDDD",
                                        "incoming": 5000000,
                                        "outgoing": 3000000
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid identity format" },
                        "404": { "description": "Entity not found in index" }
                    }
                }
            },
            "/v1/entity/{id}/transactions": {
                "get": {
                    "operationId": "getEntityTransactions",
                    "summary": "Get transactions for an entity",
                    "description": "Returns a list of all transaction hashes associated with a specific entity (account). This includes transactions where the entity is either the sender or the recipient.\n\nThe returned hashes can be used with the `/v1/tx/{hash}` endpoint to retrieve full transaction details. Results may be limited; check the `ticks_behind` field in `/system-info` to know if more data is being indexed.",
                    "tags": ["entities"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "description": "Base26-encoded entity identity",
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of transaction hashes for this entity",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "type": "string", "description": "64-character hex transaction hash" }
                                    },
                                    "example": [
                                        "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                                        "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
                                    ]
                                }
                            }
                        },
                        "400": { "description": "Invalid identity format" },
                        "404": { "description": "Entity not found in index" }
                    }
                }
            },
            "/v1/computors": {
                "get": {
                    "operationId": "getComputors",
                    "summary": "Get latest computors list",
                    "description": "Returns the list of computor public keys for the current epoch. Computors are special nodes on the Qubic network responsible for running the decentralized computation quorum. There are 676 computors in total per epoch.\n\nThis endpoint returns the most recently indexed computor set. To get computors for a historical epoch, use the `/v1/computors/{epoch}` endpoint instead.",
                    "tags": ["computors"],
                    "responses": {
                        "200": {
                            "description": "Computors data for the current epoch",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Computors" },
                                    "example": {
                                        "epoch": 218,
                                        "public_keys": [
                                            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                                            "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
                                        ]
                                    }
                                }
                            }
                        },
                        "404": { "description": "No computors data available yet" }
                    }
                }
            },
            "/v1/computors/{epoch}": {
                "get": {
                    "operationId": "getComputorsByEpoch",
                    "summary": "Get computors for a specific epoch",
                    "description": "Returns the list of computor public keys for a specified epoch. Each epoch in Qubic lasts approximately 1 week. The computors for each epoch are elected through the network's consensus mechanism.\n\nEpoch numbers start from 0. Use this endpoint to look up the historical computor set for any past epoch.",
                    "tags": ["computors"],
                    "parameters": [
                        {
                            "name": "epoch",
                            "in": "path",
                            "required": true,
                            "description": "The epoch number to query (0 to current epoch)",
                            "schema": { "type": "integer", "format": "uint16", "minimum": 0 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Computors data for the requested epoch",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Computors" },
                                    "example": {
                                        "epoch": 217,
                                        "public_keys": [
                                            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                                        ]
                                    }
                                }
                            }
                        },
                        "404": { "description": "No computors data found for this epoch" }
                    }
                }
            },
            "/v1/issued-assets": {
                "get": {
                    "operationId": "getIssuedAssets",
                    "summary": "List issued assets",
                    "description": "Returns a list of all assets that have been issued on the Qubic network. Assets represent fungible tokens that can be created, transferred, and possessed by entities (accounts).\n\nEach asset record includes metadata such as the issuer, asset name, and current supply. This endpoint returns the complete list of issued assets as of the last indexed tick.",
                    "tags": ["assets"],
                    "responses": {
                        "200": {
                            "description": "List of all issued assets",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Asset" }
                                    },
                                    "example": [
                                        {
                                            "index": 0,
                                            "issuer": "AAAAABBBBBCCCCCDDDDD",
                                            "name": "MyToken"
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "/v1/assets/{index}": {
                "get": {
                    "operationId": "getAsset",
                    "summary": "Get asset by index",
                    "description": "Retrieves detailed information about a specific asset by its numeric index. Asset indices are sequential integers assigned when assets are issued.\n\nReturns full asset metadata including issuer identity, asset name, current supply, and any other stored properties.",
                    "tags": ["assets"],
                    "parameters": [
                        {
                            "name": "index",
                            "in": "path",
                            "required": true,
                            "description": "The asset index (non-negative integer)",
                            "schema": { "type": "integer", "minimum": 0 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Asset data found and returned",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Asset" },
                                    "example": {
                                        "index": 0,
                                        "issuer": "AAAAABBBBBCCCCCDDDDD",
                                        "name": "MyToken"
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid asset index: must be a non-negative integer" },
                        "404": { "description": "Asset not found" }
                    }
                }
            },
            "/v1/owned-assets/{id}": {
                "get": {
                    "operationId": "getOwnedAssets",
                    "summary": "Get assets owned by an entity",
                    "description": "Returns a list of assets where the specified entity is the issuer (owner). An entity 'owns' an asset if it originally issued that asset on the network.\n\nThis is distinct from 'possessed' assets, which are assets the entity currently holds in its balance regardless of who issued them.",
                    "tags": ["assets"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "description": "Base26-encoded entity identity",
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of assets owned (issued) by this entity",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Asset" }
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid identity format" }
                    }
                }
            },
            "/v1/possessed-assets/{id}": {
                "get": {
                    "operationId": "getPossessedAssets",
                    "summary": "Get assets possessed by an entity",
                    "description": "Returns a list of assets currently held (possessed) by the specified entity. This includes all assets in the entity's balance, regardless of who originally issued them.\n\nThis is useful for building portfolio views or balance displays for any account on the Qubic network.",
                    "tags": ["assets"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "description": "Base26-encoded entity identity",
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of assets possessed by this entity",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Asset" }
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid identity format" }
                    }
                }
            },
            "/v1/contract-ipo/{index}": {
                "get": {
                    "operationId": "getContractIpo",
                    "summary": "Get contract IPO data",
                    "description": "Retrieves the Initial Public Offering (IPO) data for a specific smart contract on the Qubic network. Contract IPOs allow new smart contracts to be deployed and funded by the community.\n\nReturns the IPO details including the contract index, funding status, and associated metadata. IPOs have a defined lifecycle and may be active or closed.",
                    "tags": ["contracts"],
                    "parameters": [
                        {
                            "name": "index",
                            "in": "path",
                            "required": true,
                            "description": "The contract IPO index (non-negative integer)",
                            "schema": { "type": "integer", "minimum": 0 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Contract IPO data found and returned",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ContractIpo" },
                                    "example": {
                                        "index": 1,
                                        "contract_index": 100,
                                        "funding_target": 1000000000
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid contract IPO index: must be a non-negative integer" },
                        "404": { "description": "Contract IPO not found" }
                    }
                }
            },
            "/v1/active-ipos": {
                "get": {
                    "operationId": "getActiveIpos",
                    "summary": "List active contract IPOs",
                    "description": "Returns a list of all currently active contract IPOs on the Qubic network. Active IPOs are those that are still accepting contributions and have not yet reached their funding target or expiration.\n\nThis endpoint is useful for discovering new smart contract projects seeking community funding.",
                    "tags": ["contracts"],
                    "responses": {
                        "200": {
                            "description": "List of active IPOs (may be empty if none are active)",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/ContractIpo" }
                                    },
                                    "example": [
                                        {
                                            "index": 1,
                                            "contract_index": 100,
                                            "funding_target": 1000000000
                                        }
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "/v1/search/{query}": {
                "get": {
                    "operationId": "search",
                    "summary": "Search entities and transactions",
                    "description": "Performs a search across indexed entities and transactions using the provided query string. The search matches against entity identities, transaction hashes, and other indexed fields.\n\nThe query can be a partial identity, a full/partial transaction hash, or any other relevant string. Results include both matching entities and transactions, making this a convenient all-purpose search endpoint.",
                    "tags": ["search"],
                    "parameters": [
                        {
                            "name": "query",
                            "in": "path",
                            "required": true,
                            "description": "The search query string (partial identity, transaction hash, etc.)",
                            "schema": { "type": "string", "minLength": 1 }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Search results (may be empty if no matches found)",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object", "additionalProperties": true },
                                    "example": {
                                        "entities": [],
                                        "transactions": []
                                    }
                                }
                            }
                        },
                        "400": { "description": "Empty search query" }
                    }
                }
            },
            "/v1/spectrum/{id}": {
                "get": {
                    "operationId": "getSpectrumEntry",
                    "summary": "Get spectrum entry by identity",
                    "description": "Returns the spectrum (balance ledger) entry for a specific entity identity. The spectrum represents the complete balance history of an entity on the Qubic network.\n\nThis provides low-level access to the entity's internal ledger state, which can be useful for advanced analysis and auditing.",
                    "tags": ["entities"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "description": "Base26-encoded entity identity",
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Spectrum entry data",
                            "content": {
                                "application/json": {
                                    "schema": { "type": "object", "additionalProperties": true },
                                    "example": {
                                        "identity": "AAAAABBBBBCCCCCDDDDD",
                                        "balances": {}
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid identity format" },
                        "404": { "description": "Spectrum entry not found for this identity" }
                    }
                }
            },
            "/rpc": {
                "post": {
                    "operationId": "jsonRpc",
                    "summary": "JSON-RPC 2.0 endpoint",
                    "description": "Accepts JSON-RPC 2.0 requests and proxies them to the underlying Qubic network node. This endpoint allows calling any RPC method supported by the connected Qubic node, providing full node-level access through the Qonduit server.\n\nSupports both single requests and batch requests (send a JSON array of request objects). The response format follows the JSON-RPC 2.0 specification with either a `result` field on success or an `error` field on failure.",
                    "tags": ["rpc"],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": "#/components/schemas/JsonRpcRequest" },
                                "examples": {
                                    "single": {
                                        "summary": "Single RPC request",
                                        "value": {
                                            "jsonrpc": "2.0",
                                            "method": "getTickData",
                                            "params": [60349088],
                                            "id": 1
                                        }
                                    },
                                    "batch": {
                                        "summary": "Batch RPC request",
                                        "value": [
                                            { "jsonrpc": "2.0", "method": "getTickData", "params": [60349088], "id": 1 },
                                            { "jsonrpc": "2.0", "method": "getTickData", "params": [60349089], "id": 2 }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "JSON-RPC response (or batch of responses)",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/JsonRpcResponse" },
                                    "example": {
                                        "jsonrpc": "2.0",
                                        "result": {},
                                        "id": 1
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid JSON-RPC request format" },
                        "500": { "description": "Internal RPC proxy error" }
                    }
                }
            },
            "/metrics": {
                "get": {
                    "operationId": "getMetrics",
                    "summary": "Prometheus metrics",
                    "description": "Returns server metrics in Prometheus text exposition format. This endpoint is designed for integration with Prometheus monitoring systems.\n\nMetrics include request counts, latency histograms, indexing progress, ingestion pipeline health, and other operational metrics. The exact set of metrics depends on the server configuration and current state.",
                    "tags": ["system"],
                    "responses": {
                        "200": {
                            "description": "Prometheus text format metrics",
                            "content": {
                                "text/plain": {
                                    "schema": { "type": "string" },
                                    "example": "# HELP qonduit_ticks_indexed_total Total ticks indexed\n# TYPE qonduit_ticks_indexed_total counter\nqonduit_ticks_indexed_total 12345678\n"
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "HealthResponse": {
                    "type": "object",
                    "description": "Server health status response",
                    "properties": {
                        "status": { "type": "string", "description": "Health status (\"ok\" if healthy)" },
                        "version": { "type": "string", "description": "Server version string" },
                        "uptime_seconds": { "type": "integer", "description": "Server uptime in seconds" }
                    }
                },
                "SystemInfoResponse": {
                    "type": "object",
                    "description": "Detailed system and indexing pipeline status",
                    "properties": {
                        "pipeline_status": {
                            "type": "string",
                            "enum": ["live", "catching_up", "disconnected", "query_only"],
                            "description": "Current pipeline state: live=catching up in real-time, catching_up=processing backlog, disconnected=no node connection, query_only=REST API only"
                        },
                        "ingestion_connected": { "type": "boolean", "description": "Whether the ingestion pipeline is connected to a Qubic node" },
                        "node_tick": { "type": "integer", "format": "uint32", "description": "Latest tick number reported by the connected node" },
                        "node_epoch": { "type": "integer", "format": "uint16", "description": "Current epoch on the connected node" },
                        "indexed_tick": { "type": "integer", "format": "uint32", "description": "Latest tick number that has been indexed and is queryable" },
                        "indexed_epoch": { "type": "integer", "format": "uint16", "description": "Epoch corresponding to the latest indexed tick" },
                        "ticks_behind": { "type": "integer", "description": "Number of ticks the indexer is behind the live node (0 = fully caught up)" },
                        "ticks_indexed": { "type": "integer", "description": "Total number of ticks indexed since startup" },
                        "txs_indexed": { "type": "integer", "description": "Total number of transactions indexed since startup" },
                        "entities_indexed": { "type": "integer", "description": "Total number of unique entities discovered" },
                        "uptime_seconds": { "type": "integer", "description": "Server uptime in seconds" },
                        "version": { "type": "string", "description": "Server version string" }
                    }
                },
                "TickData": {
                    "type": "object",
                    "description": "Full tick data including consensus metadata, epoch, timestamp, computor information, and transaction summary",
                    "additionalProperties": true
                },
                "Transaction": {
                    "type": "object",
                    "description": "A single transaction on the Qubic network",
                    "properties": {
                        "hash": { "type": "string", "description": "64-character hex-encoded transaction hash (unique identifier)" },
                        "source_hex": { "type": "string", "description": "64-character hex-encoded public key of the sender" },
                        "source_identity": { "type": "string", "description": "Base26-encoded identity of the sender" },
                        "destination_hex": { "type": "string", "description": "64-character hex-encoded public key of the recipient" },
                        "destination_identity": { "type": "string", "description": "Base26-encoded identity of the recipient" },
                        "amount": { "type": "integer", "description": "Transfer amount in the smallest unit (1 Qubic = 1,000,000 units)" },
                        "tick": { "type": "integer", "format": "uint32", "description": "Tick number in which this transaction was included" },
                        "input_type": { "type": "integer", "description": "Type of input data (0 = simple transfer, >0 = smart contract interaction)" },
                        "input_size": { "type": "integer", "description": "Size of the input payload in bytes" }
                    }
                },
                "Entity": {
                    "type": "object",
                    "description": "Account/entity data on the Qubic network",
                    "properties": {
                        "identity": { "type": "string", "description": "Base26-encoded entity identity string" },
                        "incoming": { "type": "integer", "description": "Total cumulative incoming transaction amount" },
                        "outgoing": { "type": "integer", "description": "Total cumulative outgoing transaction amount" }
                    }
                },
                "Computors": {
                    "type": "object",
                    "description": "Set of computor public keys for an epoch",
                    "properties": {
                        "epoch": { "type": "integer", "format": "uint16", "description": "Epoch number these computors belong to" },
                        "public_keys": {
                            "type": "array",
                            "items": { "type": "string", "description": "64-character hex-encoded public key" },
                            "description": "List of computor public keys (up to 676 per epoch)"
                        }
                    }
                },
                "Asset": {
                    "type": "object",
                    "description": "An asset (fungible token) issued on the Qubic network",
                    "additionalProperties": true
                },
                "ContractIpo": {
                    "type": "object",
                    "description": "Initial Public Offering data for a smart contract deployment",
                    "additionalProperties": true
                },
                "JsonRpcRequest": {
                    "type": "object",
                    "description": "A JSON-RPC 2.0 request (or use an array for batch requests)",
                    "required": ["jsonrpc", "method"],
                    "properties": {
                        "jsonrpc": { "type": "string", "description": "JSON-RPC protocol version, must be \"2.0\"" },
                        "method": { "type": "string", "description": "The RPC method name to invoke on the Qubic node" },
                        "params": {
                            "description": "Method parameters (array or object, depending on the method)",
                            "oneOf": [
                                { "type": "array" },
                                { "type": "object" }
                            ]
                        },
                        "id": { "description": "Client-assigned request identifier (returned in the response for correlation)" }
                    }
                },
                "JsonRpcResponse": {
                    "type": "object",
                    "description": "A JSON-RPC 2.0 response",
                    "properties": {
                        "jsonrpc": { "type": "string", "description": "Protocol version (\"2.0\")" },
                        "result": { "description": "Successful result payload (method-dependent)" },
                        "error": {
                            "type": "object",
                            "description": "Error object (present only when the request fails)",
                            "properties": {
                                "code": { "type": "integer", "description": "Numeric error code defined by the RPC method" },
                                "message": { "type": "string", "description": "Short human-readable error description" },
                                "data": { "description": "Optional additional error data (method-dependent)" }
                            }
                        },
                        "id": { "description": "The request ID this response corresponds to" }
                    }
                }
            }
        },
        "tags": [
            { "name": "system", "description": "System health, status, and operational metrics" },
            { "name": "ticks", "description": "Blockchain tick (block) data and queries" },
            { "name": "transactions", "description": "Transaction data, lookups, and history" },
            { "name": "entities", "description": "Account/entity data, balances, and spectrum entries" },
            { "name": "computors", "description": "Computor sets and epoch-based queries" },
            { "name": "assets", "description": "Fungible token assets, issuers, and possession" },
            { "name": "contracts", "description": "Smart contract IPO data and active offerings" },
            { "name": "search", "description": "Cross-entity and transaction search functionality" },
            { "name": "rpc", "description": "JSON-RPC 2.0 proxy to the Qubic node" }
        ]
    })
}

/// Handler: serve OpenAPI JSON.
async fn openapi_json() -> Response {
    let spec = openapi_spec();
    Json(spec).into_response()
}

/// Comprehensive API documentation HTML page.
const DOCS_PAGE: &str = include_str!("docs_page.html");

/// Handler: serve comprehensive API docs page.
async fn guide_page() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        DOCS_PAGE,
    )
        .into_response()
}

/// Handler: serve Scalar API docs HTML at `/docs`.
async fn scalar_docs() -> Response {
    let configuration = serde_json::json!({
        "url": "/openapi.json",
        "theme": "purple",
        "layout": "modern"
    });
    let html = scalar_api_reference::scalar_html_default(&configuration);
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

/// Additional routes for API docs.
pub fn docs_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/docs", get(scalar_docs))
        .route("/guide", get(guide_page))
}
