"""Load settings from environment and optional .env file."""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        return default


@dataclass
class Settings:
    tenant_id: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[str]
    fallback_http_url: str
    auth_header_name: str
    auth_header_value: str
    http_timeout_iso: str
    azure_openai_endpoint: Optional[str]
    azure_openai_api_key: Optional[str]
    azure_openai_deployment: str
    azure_openai_api_version: str
    max_remediation_attempts: int
    rag_enabled: bool
    rag_top_k: int
    rag_embedding_deployment: Optional[str]
    rag_knowledge_path: Optional[str]
    # Multi-flow settings
    subscription_id: Optional[str]
    resource_group: Optional[str]
    log_analytics_workspace_id: Optional[str]
    multi_flow_enabled: bool
    lookback_hours: int
    top_n_runs: int
    max_concurrency: int
    schedule_minutes: int
    log_only: bool
    log_level: str


def get_settings() -> Settings:
    return Settings(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        fallback_http_url=os.getenv(
            "REMEDIATION_FALLBACK_HTTP_URL", "https://httpbin.org/status/200"
        ),
        auth_header_name=os.getenv("REMEDIATION_AUTH_HEADER_NAME", "Authorization"),
        auth_header_value=os.getenv("REMEDIATION_AUTH_HEADER_VALUE", ""),
        http_timeout_iso=os.getenv("REMEDIATION_HTTP_TIMEOUT", "PT2M"),
        azure_openai_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        azure_openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        azure_openai_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
        azure_openai_api_version=os.getenv(
            "AZURE_OPENAI_API_VERSION", "2024-02-15-preview"
        ),
        max_remediation_attempts=max(1, int(os.getenv("MAX_REMEDIATION_ATTEMPTS", "2"))),
        rag_enabled=_env_bool("RAG_ERROR_ANALYSIS", False),
        rag_top_k=max(1, int(os.getenv("RAG_TOP_K", "5"))),
        rag_embedding_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
        rag_knowledge_path=os.getenv("RAG_KNOWLEDGE_PATH"),
        # Multi-flow settings
        subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID"),
        resource_group=os.getenv("AZURE_RESOURCE_GROUP"),
        log_analytics_workspace_id=os.getenv("LOG_ANALYTICS_WORKSPACE_ID"),
        multi_flow_enabled=_env_bool("MULTI_FLOW_ENABLED", False),
        lookback_hours=_env_int("LOOKBACK_HOURS", 168),
        top_n_runs=_env_int("TOP_N_RUNS", 20),
        max_concurrency=_env_int("MAX_CONCURRENCY", 4),
        schedule_minutes=_env_int("SCHEDULE_MINUTES", 0),
        log_only=_env_bool("LOG_ONLY", False),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )