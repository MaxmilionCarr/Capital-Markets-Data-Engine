Market Researcher
Overview

Market Researcher is a Python-based platform for collecting, storing and analysing market data from multiple sources.
It is designed for quantitative research and trading strategy development—allowing you to:

Pull historical and real-time market data from Interactive Brokers (IBKR) and optionally Yahoo Finance as a fallback.

Store and organise data in a relational SQLite database with well-defined schemas for exchanges, markets, tickers, equities, bonds, historical prices and options.

Provide a modular CRUD layer so that higher-level analytics or backtesting tools can query and update the database cleanly.

This repository focuses on data engineering for research, not automated trading execution.

Features

Multi-source data fetching
IBKR TWS/Gateway API, FMP REST API

Structured database schema
Exchanges, Markets and Tickers

Equities and Bonds specific tables

Historical Prices and Option Chains with price history

Foreign keys with cascading deletes and uniqueness constraints.

Modular CRUD design
Repository classes (e.g. ExchangeRepository) encapsulate Create, Read, Update and Delete logic for each table.
These classes enforce type checking and input validation for reliable database operations.

Extendable for research
The design allows you to later incorporate financial statements, DCF models or other analytics without major refactoring.

*Still Being Updated*
