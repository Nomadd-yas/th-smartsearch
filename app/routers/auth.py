import logging
import sqlite3

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import current_user
from app.schemas.auth import (
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    RegisterRequest,
    TokenResponse,
)
from utils.auth import (
    create_access_token,
    create_refresh_token,
    create_user,
    get_user,
    get_user_by_id,
    revoke_refresh_token,
    rotate_refresh_token,
    verify_password,
)

router = APIRouter(prefix="/auth", tags=["auth"])
log = logging.getLogger("smartsearch")


@router.post("/register", status_code=201, summary="Регистрация")
def register(req: RegisterRequest) -> dict:
    try:
        create_user(req.username, req.password)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail="Пользователь уже существует")
    log.info("POST /auth/register  username=%r", req.username)
    return {"message": "Пользователь зарегистрирован"}


@router.post("/login", response_model=TokenResponse, summary="Вход")
def login(req: LoginRequest) -> TokenResponse:
    user = get_user(req.username)
    if not user or not verify_password(req.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")
    access = create_access_token(user["id"], user["username"])
    refresh = create_refresh_token(user["id"])
    log.info("POST /auth/login  username=%r", req.username)
    return TokenResponse(access_token=access, refresh_token=refresh)


@router.post("/refresh", response_model=TokenResponse, summary="Обновление access token")
def refresh(req: RefreshRequest) -> TokenResponse:
    result = rotate_refresh_token(req.refresh_token)
    if not result:
        raise HTTPException(status_code=401, detail="Refresh token недействителен или истёк")
    user_id, new_refresh = result
    user = get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="Пользователь не найден")
    access = create_access_token(user["id"], user["username"])
    log.info("POST /auth/refresh  user_id=%d", user_id)
    return TokenResponse(access_token=access, refresh_token=new_refresh)


@router.post("/logout", summary="Выход")
def logout(req: LogoutRequest, _: dict = Depends(current_user)) -> dict:
    revoke_refresh_token(req.refresh_token)
    log.info("POST /auth/logout")
    return {"message": "Выход выполнен"}
