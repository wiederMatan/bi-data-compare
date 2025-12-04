"""Authentication API routes."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, EmailStr, Field

from src.core.logging import get_logger
from src.services.auth import AuthService, User, get_auth_service

logger = get_logger(__name__)
router = APIRouter()


class RegisterRequest(BaseModel):
    """User registration request."""

    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8)


class LoginRequest(BaseModel):
    """User login request."""

    username: str
    password: str


class TokenResponse(BaseModel):
    """Token response."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int = 86400  # 24 hours


class UserResponse(BaseModel):
    """User response."""

    user_id: int
    username: str
    email: str
    role: str
    is_active: bool


class APIKeyRequest(BaseModel):
    """API key creation request."""

    name: str = Field("default", description="Key name")
    expires_in_days: Optional[int] = Field(None, description="Days until expiration")


class ChangePasswordRequest(BaseModel):
    """Change password request."""

    current_password: str
    new_password: str = Field(..., min_length=8)


def get_current_user(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
) -> User:
    """Get current authenticated user from token or API key."""
    auth_service = get_auth_service()

    # Try API key first
    if x_api_key:
        user = auth_service.verify_api_key(x_api_key)
        if user:
            return user

    # Try JWT token
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        payload = auth_service.verify_token(token)
        if payload:
            user = auth_service.get_user(payload["user_id"])
            if user:
                return user

    raise HTTPException(
        status_code=401,
        detail="Invalid or missing authentication",
        headers={"WWW-Authenticate": "Bearer"},
    )


@router.post("/register", response_model=UserResponse)
async def register(request: RegisterRequest):
    """
    Register a new user.

    Creates a new user account with the provided credentials.
    """
    auth_service = get_auth_service()

    user = auth_service.create_user(
        username=request.username,
        email=request.email,
        password=request.password,
    )

    if not user:
        raise HTTPException(
            status_code=400,
            detail="Username or email already exists",
        )

    logger.info(f"User registered: {user.username}")
    return UserResponse(
        user_id=user.user_id,
        username=user.username,
        email=user.email,
        role=user.role,
        is_active=user.is_active,
    )


@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest):
    """
    Login and get access token.

    Authenticates user and returns JWT token for subsequent requests.
    """
    auth_service = get_auth_service()

    user = auth_service.authenticate(request.username, request.password)

    if not user:
        raise HTTPException(
            status_code=401,
            detail="Invalid username or password",
        )

    token = auth_service.generate_token(user)
    logger.info(f"User logged in: {user.username}")

    return TokenResponse(access_token=token)


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """
    Get current user info.

    Returns the authenticated user's profile information.
    """
    return UserResponse(
        user_id=current_user.user_id,
        username=current_user.username,
        email=current_user.email,
        role=current_user.role,
        is_active=current_user.is_active,
    )


@router.post("/api-keys")
async def create_api_key(
    request: APIKeyRequest,
    current_user: User = Depends(get_current_user),
):
    """
    Create a new API key.

    Generates an API key for programmatic access.
    """
    auth_service = get_auth_service()

    api_key = auth_service.create_api_key(
        user_id=current_user.user_id,
        name=request.name,
        expires_in_days=request.expires_in_days,
    )

    if not api_key:
        raise HTTPException(
            status_code=500,
            detail="Failed to create API key",
        )

    return {
        "api_key": api_key,
        "name": request.name,
        "message": "Store this key securely. It will not be shown again.",
    }


@router.post("/change-password")
async def change_password(
    request: ChangePasswordRequest,
    current_user: User = Depends(get_current_user),
):
    """
    Change user password.

    Updates the authenticated user's password.
    """
    auth_service = get_auth_service()

    # Verify current password
    user = auth_service.authenticate(current_user.username, request.current_password)
    if not user:
        raise HTTPException(
            status_code=400,
            detail="Current password is incorrect",
        )

    success = auth_service.change_password(current_user.user_id, request.new_password)

    if not success:
        raise HTTPException(
            status_code=500,
            detail="Failed to change password",
        )

    logger.info(f"Password changed for user: {current_user.username}")
    return {"message": "Password changed successfully"}


@router.get("/users", response_model=list[UserResponse])
async def list_users(current_user: User = Depends(get_current_user)):
    """
    List all users (admin only).

    Returns list of all registered users.
    """
    if current_user.role != "admin":
        raise HTTPException(
            status_code=403,
            detail="Admin access required",
        )

    auth_service = get_auth_service()
    users = auth_service.list_users()

    return [
        UserResponse(
            user_id=u.user_id,
            username=u.username,
            email=u.email,
            role=u.role,
            is_active=u.is_active,
        )
        for u in users
    ]


@router.post("/users/{user_id}/deactivate")
async def deactivate_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """
    Deactivate a user (admin only).

    Disables a user account.
    """
    if current_user.role != "admin":
        raise HTTPException(
            status_code=403,
            detail="Admin access required",
        )

    if user_id == current_user.user_id:
        raise HTTPException(
            status_code=400,
            detail="Cannot deactivate yourself",
        )

    auth_service = get_auth_service()
    success = auth_service.deactivate_user(user_id)

    if not success:
        raise HTTPException(
            status_code=404,
            detail="User not found",
        )

    logger.info(f"User deactivated: {user_id} by {current_user.username}")
    return {"message": "User deactivated successfully"}
