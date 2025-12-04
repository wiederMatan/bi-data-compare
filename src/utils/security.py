"""Security utility functions for credential management."""

import base64
import os
from typing import Optional

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from src.core.logging import get_logger

logger = get_logger(__name__)


def generate_key(password: str, salt: Optional[bytes] = None) -> tuple[bytes, bytes]:
    """
    Generate encryption key from password.

    Args:
        password: Password to derive key from
        salt: Optional salt (will be generated if not provided)

    Returns:
        Tuple of (key, salt)
    """
    if salt is None:
        salt = os.urandom(16)

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
    return key, salt


def encrypt_value(value: str, key: bytes) -> str:
    """
    Encrypt a string value.

    Args:
        value: Value to encrypt
        key: Encryption key

    Returns:
        Encrypted value (base64 encoded)
    """
    try:
        f = Fernet(key)
        encrypted = f.encrypt(value.encode())
        return base64.urlsafe_b64encode(encrypted).decode()
    except Exception as e:
        logger.error(f"Encryption failed: {str(e)}")
        raise


def decrypt_value(encrypted_value: str, key: bytes) -> str:
    """
    Decrypt a string value.

    Args:
        encrypted_value: Encrypted value (base64 encoded)
        key: Encryption key

    Returns:
        Decrypted value
    """
    try:
        f = Fernet(key)
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_value.encode())
        decrypted = f.decrypt(encrypted_bytes)
        return decrypted.decode()
    except Exception as e:
        logger.error(f"Decryption failed: {str(e)}")
        raise


class CredentialManager:
    """Manages encrypted credentials."""

    def __init__(self, master_password: Optional[str] = None) -> None:
        """
        Initialize credential manager.

        Args:
            master_password: Master password for encryption
        """
        if master_password:
            self.key, self.salt = generate_key(master_password)
        else:
            # Generate a random key
            self.key = Fernet.generate_key()
            self.salt = None

        self._credentials: dict[str, str] = {}

    def store_credential(self, name: str, value: str, encrypt: bool = True) -> None:
        """
        Store a credential.

        Args:
            name: Credential name
            value: Credential value
            encrypt: Whether to encrypt the value
        """
        if encrypt:
            encrypted_value = encrypt_value(value, self.key)
            self._credentials[name] = encrypted_value
            logger.debug(f"Stored encrypted credential: {name}")
        else:
            self._credentials[name] = value
            logger.debug(f"Stored credential: {name}")

    def get_credential(self, name: str, encrypted: bool = True) -> Optional[str]:
        """
        Retrieve a credential.

        Args:
            name: Credential name
            encrypted: Whether the value is encrypted

        Returns:
            Credential value or None if not found
        """
        value = self._credentials.get(name)
        if value is None:
            return None

        if encrypted:
            try:
                return decrypt_value(value, self.key)
            except Exception as e:
                logger.error(f"Failed to decrypt credential {name}: {str(e)}")
                return None
        return value

    def remove_credential(self, name: str) -> None:
        """
        Remove a credential.

        Args:
            name: Credential name
        """
        if name in self._credentials:
            del self._credentials[name]
            logger.debug(f"Removed credential: {name}")

    def clear_all(self) -> None:
        """Clear all stored credentials."""
        self._credentials.clear()
        logger.debug("Cleared all credentials")

    def has_credential(self, name: str) -> bool:
        """
        Check if credential exists.

        Args:
            name: Credential name

        Returns:
            True if credential exists
        """
        return name in self._credentials


# Global credential manager instance
_credential_manager: Optional[CredentialManager] = None


def get_credential_manager() -> CredentialManager:
    """
    Get global credential manager instance.

    Returns:
        Credential manager instance
    """
    global _credential_manager
    if _credential_manager is None:
        _credential_manager = CredentialManager()
    return _credential_manager
