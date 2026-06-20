//! OpenAPI spec and Scalar API docs UI.
//!
//! Serves the OpenAPI JSON at `/openapi.json` and an interactive
//! Scalar API reference UI at `/docs`.

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
            "description": "Qubic blockchain indexer and RPC server",
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
                    "tags": ["system"],
                    "responses": {
                        "200": {
                            "description": "Server is healthy",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/HealthResponse" }
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
                    "tags": ["system"],
                    "responses": {
                        "200": {
                            "description": "Pipeline and system status",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/SystemInfoResponse" }
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
                    "tags": ["ticks"],
                    "responses": {
                        "200": {
                            "description": "Latest tick data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/TickData" }
                                }
                            }
                        },
                        "404": { "description": "No tick data available" }
                    }
                }
            },
            "/v1/tick/{tick}": {
                "get": {
                    "operationId": "getTick",
                    "summary": "Get tick data by number",
                    "tags": ["ticks"],
                    "parameters": [
                        {
                            "name": "tick",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "integer", "format": "uint32" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Tick data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/TickData" }
                                }
                            }
                        },
                        "404": { "description": "Tick not found" }
                    }
                }
            },
            "/v1/tick/{tick}/tx": {
                "get": {
                    "operationId": "getTickTransactions",
                    "summary": "Get transactions in a tick",
                    "tags": ["transactions"],
                    "parameters": [
                        {
                            "name": "tick",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "integer", "format": "uint32" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of transactions",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Transaction" }
                                    }
                                }
                            }
                        },
                        "404": { "description": "Tick not found" }
                    }
                }
            },
            "/v1/tx/{hash}": {
                "get": {
                    "operationId": "getTransaction",
                    "summary": "Get transaction by hash",
                    "tags": ["transactions"],
                    "parameters": [
                        {
                            "name": "hash",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Transaction data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Transaction" }
                                }
                            }
                        },
                        "404": { "description": "Transaction not found" }
                    }
                }
            },
            "/v1/entity/{id}": {
                "get": {
                    "operationId": "getEntity",
                    "summary": "Get entity (account) data",
                    "tags": ["entities"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "description": "Base26 encoded identity",
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Entity data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Entity" }
                                }
                            }
                        },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Entity not found" }
                    }
                }
            },
            "/v1/entity/{id}/transactions": {
                "get": {
                    "operationId": "getEntityTransactions",
                    "summary": "Get transactions for an entity",
                    "tags": ["entities"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of transaction hashes",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "type": "string" }
                                    }
                                }
                            }
                        },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Entity not found" }
                    }
                }
            },
            "/v1/computors": {
                "get": {
                    "operationId": "getComputors",
                    "summary": "Get latest computors list",
                    "tags": ["computors"],
                    "responses": {
                        "200": {
                            "description": "Computors data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Computors" }
                                }
                            }
                        },
                        "404": { "description": "No computors data" }
                    }
                }
            },
            "/v1/computors/{epoch}": {
                "get": {
                    "operationId": "getComputorsByEpoch",
                    "summary": "Get computors for a specific epoch",
                    "tags": ["computors"],
                    "parameters": [
                        {
                            "name": "epoch",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "integer", "format": "uint16" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Computors data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Computors" }
                                }
                            }
                        },
                        "404": { "description": "No computors for this epoch" }
                    }
                }
            },
            "/v1/issued-assets": {
                "get": {
                    "operationId": "getIssuedAssets",
                    "summary": "List issued assets",
                    "tags": ["assets"],
                    "responses": {
                        "200": {
                            "description": "List of assets",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": { "$ref": "#/components/schemas/Asset" }
                                    }
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
                    "tags": ["assets"],
                    "parameters": [
                        {
                            "name": "index",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "integer" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Asset data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/Asset" }
                                }
                            }
                        },
                        "404": { "description": "Asset not found" }
                    }
                }
            },
            "/v1/owned-assets/{id}": {
                "get": {
                    "operationId": "getOwnedAssets",
                    "summary": "Get assets owned by an entity",
                    "tags": ["assets"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": { "description": "Owned assets" },
                        "400": { "description": "Invalid identity" }
                    }
                }
            },
            "/v1/possessed-assets/{id}": {
                "get": {
                    "operationId": "getPossessedAssets",
                    "summary": "Get assets possessed by an entity",
                    "tags": ["assets"],
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": { "description": "Possessed assets" },
                        "400": { "description": "Invalid identity" }
                    }
                }
            },
            "/v1/contract-ipo/{index}": {
                "get": {
                    "operationId": "getContractIpo",
                    "summary": "Get contract IPO data",
                    "tags": ["contracts"],
                    "parameters": [
                        {
                            "name": "index",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "integer" }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Contract IPO data",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": "#/components/schemas/ContractIpo" }
                                }
                            }
                        },
                        "404": { "description": "Contract IPO not found" }
                    }
                }
            },
            "/v1/search/{query}": {
                "get": {
                    "operationId": "search",
                    "summary": "Search entities and transactions",
                    "tags": ["search"],
                    "parameters": [
                        {
                            "name": "query",
                            "in": "path",
                            "required": true,
                            "schema": { "type": "string" }
                        }
                    ],
                    "responses": {
                        "200": { "description": "Search results" }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "HealthResponse": {
                    "type": "object",
                    "properties": {
                        "status": { "type": "string" },
                        "version": { "type": "string" },
                        "uptime_seconds": { "type": "integer" }
                    }
                },
                "SystemInfoResponse": {
                    "type": "object",
                    "properties": {
                        "pipeline_status": {
                            "type": "string",
                            "enum": ["live", "catching_up", "disconnected", "query_only"]
                        },
                        "ingestion_connected": { "type": "boolean" },
                        "node_tick": { "type": "integer", "format": "uint32" },
                        "node_epoch": { "type": "integer", "format": "uint16" },
                        "indexed_tick": { "type": "integer", "format": "uint32" },
                        "indexed_epoch": { "type": "integer", "format": "uint16" },
                        "ticks_behind": { "type": "integer" },
                        "ticks_indexed": { "type": "integer" },
                        "txs_indexed": { "type": "integer" },
                        "entities_indexed": { "type": "integer" },
                        "uptime_seconds": { "type": "integer" },
                        "version": { "type": "string" }
                    }
                },
                "TickData": {
                    "type": "object",
                    "description": "Decoded tick data",
                    "additionalProperties": true
                },
                "Transaction": {
                    "type": "object",
                    "properties": {
                        "hash": { "type": "string" },
                        "source_hex": { "type": "string" },
                        "source_identity": { "type": "string" },
                        "destination_hex": { "type": "string" },
                        "destination_identity": { "type": "string" },
                        "amount": { "type": "integer" },
                        "tick": { "type": "integer", "format": "uint32" },
                        "input_type": { "type": "integer" },
                        "input_size": { "type": "integer" }
                    }
                },
                "Entity": {
                    "type": "object",
                    "properties": {
                        "identity": { "type": "string" },
                        "incoming": { "type": "integer" },
                        "outgoing": { "type": "integer" }
                    }
                },
                "Computors": {
                    "type": "object",
                    "properties": {
                        "epoch": { "type": "integer", "format": "uint16" },
                        "public_keys": {
                            "type": "array",
                            "items": { "type": "string" }
                        }
                    }
                },
                "Asset": {
                    "type": "object",
                    "description": "Asset record",
                    "additionalProperties": true
                },
                "ContractIpo": {
                    "type": "object",
                    "description": "Contract IPO data",
                    "additionalProperties": true
                }
            }
        },
        "tags": [
            { "name": "system", "description": "System health and status" },
            { "name": "ticks", "description": "Tick data" },
            { "name": "transactions", "description": "Transaction data" },
            { "name": "entities", "description": "Account/entity data" },
            { "name": "computors", "description": "Computor lists" },
            { "name": "assets", "description": "Asset records" },
            { "name": "contracts", "description": "Contract IPO data" },
            { "name": "search", "description": "Search functionality" }
        ]
    })
}

/// Handler: serve OpenAPI JSON.
async fn openapi_json() -> Response {
    let spec = openapi_spec();
    Json(spec).into_response()
}

/// Handler: serve Scalar API docs HTML.
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
}
