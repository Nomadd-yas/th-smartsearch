from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from utils.auth import verify_access_token
from utils.client import SteSearchClient

_bearer = HTTPBearer()


def get_client(request: Request) -> SteSearchClient:
    client: SteSearchClient | None = getattr(request.app.state, "client", None)
    if client is None:
        raise HTTPException(status_code=503, detail="Движок ещё не загружен")
    return client


def current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> dict:
    payload = verify_access_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail="Токен недействителен или истёк")
    return payload
