# OptAlpha Broker Server

A **FastAPI-based multi-broker trading server** that provides a unified REST API for interacting with multiple Indian stock brokers. The server abstracts away broker-specific implementations, enabling clients to place orders, view positions, manage portfolios, and more through a single consistent interface.

**Version:** 0.1 &nbsp;|&nbsp; **Python:** 3.9 &nbsp;|&nbsp; **Framework:** FastAPI + Uvicorn

---

## Supported Brokers

| Broker | Auth | Orders | Positions | Portfolio | Trading |
|--------|------|--------|-----------|-----------|---------|
| **Angel One** (SmartAPI) | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Kotak Neo** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Shoonya** (Finvasia) | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Architecture

The project follows a **modular, abstract-base-class** design where each functional domain is defined by a base class and broker-specific subclasses:

```
BrokerAuthInit (ABC)          BrokerOrd (ABC)
├── AngelAuthInit              ├── AngelOrd
├── KotakneoAuthInit           ├── KotakneoOrd
└── ShoonyaAuthInit            └── ShoonyaOrd

BrokerPos (ABC)               BrokerPortfo (ABC)          BrokerTrade (ABC)
├── AngelPos                   ├── AngelPortfo              ├── AngelTrade
├── KotakneoPos                ├── KotakneoPortfo           ├── KotakneoTrade
└── ShoonyaPos                 └── ShoonyaPortfo            └── ShoonyaTrade
```

### Infrastructure

| Component | Purpose |
|-----------|---------|
| **Redis** | Session caching, order/position dataframes, user state |
| **PostgreSQL** | Persistent user credential & login info storage |
| **Telegram Bot** | Notifications and alerts to a monitoring chat |
| **Docker** | Containerized deployment (prod & test environments) |

---

## Project Structure

```
OptAlphaBrokerServer/
├── Broker_API.py              # FastAPI app — all REST endpoints
├── BrokerAuthInit/            # Authentication & initialization
│   ├── BrokerAuthInit.py      #   Base class (ABC)
│   ├── AngelAuthInit.py       #   Angel One implementation
│   ├── KotakneoAuthInit.py    #   Kotak Neo implementation
│   └── ShoonyaAuthInit.py     #   Shoonya implementation
├── BrokerOrd/                 # Order management
│   ├── BrokerOrd.py           #   Base class (ABC)
│   ├── AngelOrd.py
│   ├── KotakneoOrd.py
│   └── ShoonyaOrd.py
├── BrokerPos/                 # Position tracking
│   ├── BrokerPos.py           #   Base class (ABC)
│   ├── AngelPos.py
│   ├── KotakneoPos.py
│   └── ShoonyaPos.py
├── BrokerPortfo/              # Portfolio management
│   ├── BrokerPortfo.py        #   Base class (ABC)
│   ├── AngelPortfo.py
│   ├── KotakneoPortfo.py
│   └── ShoonyaPortfo.py
├── BrokerTrade/               # Trading operations (place/modify/cancel)
│   ├── BrokerTrade.py         #   Base class (ABC)
│   ├── AngelTrade.py
│   ├── KotakneoTrade.py
│   └── ShoonyaTrade.py
├── BrokerData/                # Runtime data
│   ├── Logs/                  #   Daily log files
│   ├── PosOrd/                #   Position/order snapshots
│   └── Stocks/                #   Instrument lists (all.csv)
├── Tokens/                    # Token/instrument data files
├── .github/workflows/         # CI/CD pipeline
│   └── docker-image.yml       #   Auto-build & push Docker images
├── Dockerfile_prod            # Production image (Uvicorn on port 7777)
├── Dockerfile_test            # Test/dev image (Jupyter on port 8888)
├── requirements.txt           # Python dependencies
├── version.txt                # Semver-style version tracker
└── NorenRestApi-0.0.30-*.whl  # Shoonya/NorenRestApi wheel
```

---

## API Reference

All endpoints accept **POST** requests with JSON body (unless noted). Every response includes an `error` field — empty string on success, error message on failure.

### Authentication

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/get_file` | Retrieve user credentials from PostgreSQL |
| POST | `/login` | Authenticate with the user's broker |

### Token / Instrument Lookup

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/get_token` | Get instrument token by name, exchange, expiry, strike, option type |
| POST | `/get_name` | Reverse-lookup instrument details from a token |

### Orders

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/orders` | Fetch all orders for a user |
| POST | `/order_update_time` | Get last order-update timestamp |

### Positions

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/positions` | Fetch all open positions for a user |
| POST | `/position_update_time` | Get last position-update timestamp |

### Portfolio

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/portfolio` | Fetch portfolio holdings for a user |

### Trading

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/get_available_cash` | Get available cash/margin for a user |
| POST | `/get_required_margin` | Calculate required margin for a trade |
| POST | `/get_quote` | Get live quote for an instrument |
| POST | `/place_order` | Place a new order |
| POST | `/modify_order` | Modify an existing order |
| POST | `/cancel_order` | Cancel an existing order |

### Health Check

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Returns a welcome message |

---

## Environment Variables

The following environment variables must be set (typically via Kubernetes secrets or Docker env):

| Variable | Description |
|----------|-------------|
| `host` | PostgreSQL host address |
| `port_postgres` | PostgreSQL port |
| `username` | PostgreSQL username |
| `password` | PostgreSQL password |
| `token` | Telegram Bot API token |

The app also expects a **Redis** instance at `redis-service:6379`.

---

## Getting Started

### Prerequisites

- Python 3.9+
- Redis server
- PostgreSQL database
- Telegram Bot token (for notifications)

### Local Setup

```bash
# Install dependencies
pip install NorenRestApi-0.0.30-py2.py3-none-any.whl
pip install git+https://github.com/Kotak-Neo/kotak-neo-api.git#egg=neo_api_client
pip install -r requirements.txt

# Set environment variables (see table above)
export host=<postgres_host>
export port_postgres=<postgres_port>
export username=<postgres_user>
export password=<postgres_password>
export token=<telegram_bot_token>

# Run the server
uvicorn Broker_API:app --host 0.0.0.0 --port 7777
```

### Docker

**Production** — runs the FastAPI server on port **7777**:

```bash
docker build -t broker-api -f Dockerfile_prod .
docker run -p 7777:7777 --env-file .env broker-api
```

**Test / Development** — runs Jupyter Notebook on port **8888**:

```bash
docker build -t broker-test -f Dockerfile_test .
docker run -p 8888:8888 --env-file .env broker-test
```

---

## CI/CD

A **GitHub Actions** workflow (`.github/workflows/docker-image.yml`) automates the build and deployment pipeline:

1. Triggers on push/PR to `master`
2. Reads the current version from `version.txt`
3. Auto-increments the version (by `0.1`)
4. Builds both `broker-api` and `broker-test` Docker images
5. Pushes images to **Docker Hub**
6. Commits the updated `version.txt` back to the repository

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `fastapi` / `uvicorn` | API framework & ASGI server |
| `redis` | Session & data caching |
| `psycopg2-binary` / `SQLAlchemy` | PostgreSQL connectivity |
| `pandas` / `numpy` | Data manipulation |
| `smartapi-python` | Angel One broker SDK |
| `neo_api_client` | Kotak Neo broker SDK |
| `NorenRestApi` | Shoonya (Finvasia) broker SDK |
| `python-telegram-bot` | Telegram notifications |
| `pyotp` | TOTP-based two-factor auth |
| `websockets` | Real-time data feeds |
| `logzero` | Logging utilities |

---

## License

*Not specified — add a `LICENSE` file to define usage terms.*
