"""User authentication service."""

import hashlib
import secrets
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Optional

import jwt

from src.core.logging import get_logger

logger = get_logger(__name__)

# JWT configuration
JWT_SECRET_KEY = secrets.token_hex(32)
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24


class User:
    """User model."""

    def __init__(
        self,
        user_id: int,
        username: str,
        email: str,
        role: str = "user",
        is_active: bool = True,
        created_at: Optional[datetime] = None,
    ):
        self.user_id = user_id
        self.username = username
        self.email = email
        self.role = role
        self.is_active = is_active
        self.created_at = created_at or datetime.now()

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "role": self.role,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class AuthService:
    """Service for user authentication and authorization."""

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize auth service.

        Args:
            db_path: Path to SQLite database for users
        """
        import os
        if db_path is None:
            config_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "config"
            )
            os.makedirs(config_dir, exist_ok=True)
            db_path = os.path.join(config_dir, "users.db")

        self.db_path = db_path
        self._local = threading.local()
        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, "connection"):
            self._local.connection = sqlite3.connect(
                self.db_path, check_same_thread=False
            )
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection

    def _init_database(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'user',
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_keys (
                key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                key_hash TEXT UNIQUE NOT NULL,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash)
        """)

        conn.commit()
        logger.info(f"Auth database initialized at {self.db_path}")

    def _hash_password(self, password: str) -> str:
        """Hash password with salt."""
        salt = secrets.token_hex(16)
        hash_val = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode(),
            salt.encode(),
            100000,
        )
        return f"{salt}:{hash_val.hex()}"

    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash."""
        try:
            salt, stored_hash = password_hash.split(":")
            computed_hash = hashlib.pbkdf2_hmac(
                "sha256",
                password.encode(),
                salt.encode(),
                100000,
            )
            return computed_hash.hex() == stored_hash
        except Exception:
            return False

    def create_user(
        self,
        username: str,
        email: str,
        password: str,
        role: str = "user",
    ) -> Optional[User]:
        """
        Create a new user.

        Args:
            username: Username
            email: Email address
            password: Plain text password
            role: User role (user, admin)

        Returns:
            User object or None if failed
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            password_hash = self._hash_password(password)

            cursor.execute(
                """
                INSERT INTO users (username, email, password_hash, role)
                VALUES (?, ?, ?, ?)
                """,
                (username, email, password_hash, role),
            )
            conn.commit()

            user_id = cursor.lastrowid
            logger.info(f"Created user: {username} (ID: {user_id})")

            return User(user_id, username, email, role)

        except sqlite3.IntegrityError as e:
            logger.error(f"Failed to create user {username}: {str(e)}")
            return None

    def authenticate(self, username: str, password: str) -> Optional[User]:
        """
        Authenticate user with username and password.

        Args:
            username: Username
            password: Plain text password

        Returns:
            User object if authenticated, None otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT * FROM users WHERE username = ? AND is_active = 1",
            (username,),
        )
        row = cursor.fetchone()

        if not row:
            logger.warning(f"Authentication failed: user {username} not found")
            return None

        if not self._verify_password(password, row["password_hash"]):
            logger.warning(f"Authentication failed: invalid password for {username}")
            return None

        # Update last login
        cursor.execute(
            "UPDATE users SET last_login = ? WHERE user_id = ?",
            (datetime.now().isoformat(), row["user_id"]),
        )
        conn.commit()

        logger.info(f"User authenticated: {username}")
        return User(
            user_id=row["user_id"],
            username=row["username"],
            email=row["email"],
            role=row["role"],
            is_active=bool(row["is_active"]),
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        )

    def generate_token(self, user: User) -> str:
        """
        Generate JWT token for user.

        Args:
            user: User object

        Returns:
            JWT token string
        """
        payload = {
            "user_id": user.user_id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS),
            "iat": datetime.utcnow(),
        }
        return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)

    def verify_token(self, token: str) -> Optional[dict]:
        """
        Verify JWT token.

        Args:
            token: JWT token string

        Returns:
            Payload dict if valid, None otherwise
        """
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token expired")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Invalid token: {str(e)}")
            return None

    def create_api_key(
        self,
        user_id: int,
        name: str = "default",
        expires_in_days: Optional[int] = None,
    ) -> Optional[str]:
        """
        Create an API key for a user.

        Args:
            user_id: User ID
            name: Key name/description
            expires_in_days: Days until expiration (None = never)

        Returns:
            API key string or None if failed
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Generate key
            api_key = f"bdc_{secrets.token_hex(32)}"
            key_hash = hashlib.sha256(api_key.encode()).hexdigest()

            expires_at = None
            if expires_in_days:
                expires_at = (datetime.now() + timedelta(days=expires_in_days)).isoformat()

            cursor.execute(
                """
                INSERT INTO api_keys (user_id, key_hash, name, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, key_hash, name, expires_at),
            )
            conn.commit()

            logger.info(f"Created API key for user {user_id}: {name}")
            return api_key

        except Exception as e:
            logger.error(f"Failed to create API key: {str(e)}")
            return None

    def verify_api_key(self, api_key: str) -> Optional[User]:
        """
        Verify API key and return associated user.

        Args:
            api_key: API key string

        Returns:
            User object if valid, None otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        key_hash = hashlib.sha256(api_key.encode()).hexdigest()

        cursor.execute(
            """
            SELECT u.* FROM users u
            JOIN api_keys k ON u.user_id = k.user_id
            WHERE k.key_hash = ?
            AND k.is_active = 1
            AND u.is_active = 1
            AND (k.expires_at IS NULL OR k.expires_at > ?)
            """,
            (key_hash, datetime.now().isoformat()),
        )
        row = cursor.fetchone()

        if not row:
            logger.warning("Invalid or expired API key")
            return None

        return User(
            user_id=row["user_id"],
            username=row["username"],
            email=row["email"],
            role=row["role"],
            is_active=bool(row["is_active"]),
        )

    def get_user(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()

        if not row:
            return None

        return User(
            user_id=row["user_id"],
            username=row["username"],
            email=row["email"],
            role=row["role"],
            is_active=bool(row["is_active"]),
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        )

    def list_users(self) -> list[User]:
        """List all users."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users ORDER BY username")

        return [
            User(
                user_id=row["user_id"],
                username=row["username"],
                email=row["email"],
                role=row["role"],
                is_active=bool(row["is_active"]),
                created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
            )
            for row in cursor.fetchall()
        ]

    def deactivate_user(self, user_id: int) -> bool:
        """Deactivate a user."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "UPDATE users SET is_active = 0 WHERE user_id = ?",
            (user_id,),
        )
        conn.commit()

        return cursor.rowcount > 0

    def change_password(self, user_id: int, new_password: str) -> bool:
        """Change user password."""
        conn = self._get_connection()
        cursor = conn.cursor()

        password_hash = self._hash_password(new_password)

        cursor.execute(
            "UPDATE users SET password_hash = ? WHERE user_id = ?",
            (password_hash, user_id),
        )
        conn.commit()

        return cursor.rowcount > 0


# Global singleton
_auth_service: Optional[AuthService] = None
_auth_lock = threading.Lock()


def get_auth_service() -> AuthService:
    """Get global auth service instance."""
    global _auth_service
    with _auth_lock:
        if _auth_service is None:
            _auth_service = AuthService()
        return _auth_service
