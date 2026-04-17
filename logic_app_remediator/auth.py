"""Azure AD access token for Azure Resource Manager."""

from typing import Optional

from azure.core.credentials import AccessToken
from azure.identity import ClientSecretCredential


def get_arm_token(
    tenant_id: Optional[str],
    client_id: Optional[str],
    client_secret: Optional[str],
) -> str:
    """
    Return a bearer token for https://management.azure.com/.
    Uses only ClientSecretCredential from env-backed service principal settings.
    """
    scope = "https://management.azure.com/.default"

    if not (tenant_id and client_id and client_secret):
        raise ValueError(
            "Missing service principal credentials. Set AZURE_TENANT_ID, "
            "AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET in .env or environment."
        )

    cred = ClientSecretCredential(tenant_id, client_id, client_secret)
    token: AccessToken = cred.get_token(scope)
    return token.token
