# DataHub

**DataHub** is a provider-agnostic financial data orchestration and
storage framework designed to ingest, enrich, and store market and
fundamental data from multiple financial data providers.

The system combines incomplete responses from different APIs to produce
**complete, canonical issuer and listing data**, while keeping the
**data provider layer fully separated from the database layer**.

------------------------------------------------------------------------

## Key Features

-   **Provider-agnostic architecture**\
    Applications interact with a single interface regardless of the
    underlying data provider.

-   **Multi-provider data enrichment**\
    Data from different providers is merged to fill missing fields.

-   **Priority-based provider routing**\
    Providers are queried in priority order and only used when required.

-   **Issuer-centric data model**\
    Companies are treated as the primary entity, with listings attached
    to them.

-   **Database abstraction layer**\
    The storage layer is isolated from data ingestion, allowing support
    for multiple database backends.

------------------------------------------------------------------------

## Project Architecture

    src/
    ├── data_providers/            # External data provider integrations
    │   ├── clients/               # API clients (IBKR, FMP, etc.)
    │   ├── services/              # Provider service wrappers
    │   └── datahub.py             # Provider orchestration layer
    │
    └── database_connector/        # Database abstraction layer
        ├── repositories/          # Data repositories
        │   ├── core/
        │   ├── instruments/
        │   ├── technical_data/
        │   └── fundamental_data/
        │
        └── db.py                  # Database connection manager

------------------------------------------------------------------------

## How DataHub Works

DataHub queries providers using a **priority routing system**:

    FMP → IBKR → Massive

The orchestration layer:

1.  Queries providers in priority order\
2.  Merges responses across providers\
3.  Fills only **missing fields**\
4.  Never overwrites higher-priority provider data

This allows DataHub to automatically construct **complete issuer and
listing records**.

------------------------------------------------------------------------

## Issuer-Centric Model

Most financial APIs are **ticker-centric**.

DataHub instead models financial data around **issuers (companies)**.

    Issuer
     ├── Exchange
     │     └── Equity Listing
     │
     └── Financial Statements

Example:

    Alphabet Inc
     ├── NASDAQ → GOOGL
     └── NASDAQ → GOOG

Issuer identity is determined using stable identifiers such as:

-   **CIK**
-   **LEI**

------------------------------------------------------------------------

## Database Connector

The `database_connector` module provides a **repository-based storage
layer** responsible for:

-   Ensuring exchanges exist
-   Creating issuers when missing
-   Creating equity listings
-   Storing historical prices
-   Storing financial statements

Repositories never interact directly with external providers.\
Instead, they rely on the **DataHub orchestration layer** to retrieve
enriched data.

Example flow:

    Repository
       ↓
    DataHub
       ↓
    Provider Services
       ↓
    Database Storage

------------------------------------------------------------------------

## Example Usage

``` python
equity = equity_repository.get_or_create_ensure(
    symbol="AAPL",
    exchange_name="NASDAQ"
)
```

Internally this will:

1.  Query providers for issuer information
2.  Enrich missing fields across providers
3.  Ensure the exchange exists
4.  Ensure the issuer exists
5.  Create the equity listing if necessary

------------------------------------------------------------------------

## Supported Data

DataHub currently supports:

-   Issuer metadata
-   Equity listings
-   Exchange information
-   Historical price data
-   Financial statements

------------------------------------------------------------------------

## Future Development

Planned improvements include:

-   Additional market data providers
-   Support for additional asset classes (ETFs, bonds, derivatives)
-   Background enrichment tasks
-   Expanded financial data coverage
-   Performance caching

### Database Support

The database connector is designed to support multiple backends.\
Future support may include:

-   PostgreSQL
-   SQLite
-   DuckDB
-   Cloud data warehouses

------------------------------------------------------------------------

## Purpose

Financial APIs frequently return **incomplete or inconsistent data**.

DataHub solves this problem by:

-   combining multiple providers
-   enriching missing data fields
-   maintaining a canonical issuer-centric data store

This makes DataHub suitable for:

-   quantitative research
-   trading systems
-   financial data pipelines
-   portfolio analytics
