"""Tests for authentication service and API."""

import pytest

from src.services.auth import AuthService, User


@pytest.fixture
def auth_service(tmp_path):
    """Create auth service with temporary database."""
    db_path = tmp_path / "test_users.db"
    return AuthService(str(db_path))


class TestUser:
    """Tests for User model."""

    def test_user_creation(self):
        """Test user model creation."""
        user = User(
            user_id=1,
            username="testuser",
            email="test@example.com",
            role="user",
        )
        assert user.user_id == 1
        assert user.username == "testuser"
        assert user.email == "test@example.com"
        assert user.role == "user"
        assert user.is_active is True

    def test_user_to_dict(self):
        """Test user serialization."""
        user = User(
            user_id=1,
            username="testuser",
            email="test@example.com",
        )
        data = user.to_dict()
        assert data["user_id"] == 1
        assert data["username"] == "testuser"
        assert data["email"] == "test@example.com"


class TestAuthService:
    """Tests for AuthService."""

    def test_create_user(self, auth_service):
        """Test user creation."""
        user = auth_service.create_user(
            username="testuser",
            email="test@example.com",
            password="password123",
        )
        assert user is not None
        assert user.username == "testuser"
        assert user.email == "test@example.com"

    def test_create_duplicate_user(self, auth_service):
        """Test duplicate user creation fails."""
        auth_service.create_user("user1", "user1@test.com", "pass123")
        user2 = auth_service.create_user("user1", "user2@test.com", "pass123")
        assert user2 is None

    def test_authenticate_valid(self, auth_service):
        """Test valid authentication."""
        auth_service.create_user("testuser", "test@example.com", "password123")
        user = auth_service.authenticate("testuser", "password123")
        assert user is not None
        assert user.username == "testuser"

    def test_authenticate_invalid_password(self, auth_service):
        """Test invalid password fails."""
        auth_service.create_user("testuser", "test@example.com", "password123")
        user = auth_service.authenticate("testuser", "wrongpassword")
        assert user is None

    def test_authenticate_invalid_user(self, auth_service):
        """Test invalid username fails."""
        user = auth_service.authenticate("nonexistent", "password123")
        assert user is None

    def test_generate_and_verify_token(self, auth_service):
        """Test JWT token generation and verification."""
        user = auth_service.create_user("testuser", "test@example.com", "password123")
        token = auth_service.generate_token(user)
        assert token is not None

        payload = auth_service.verify_token(token)
        assert payload is not None
        assert payload["username"] == "testuser"

    def test_verify_invalid_token(self, auth_service):
        """Test invalid token verification fails."""
        payload = auth_service.verify_token("invalid_token")
        assert payload is None

    def test_create_and_verify_api_key(self, auth_service):
        """Test API key creation and verification."""
        user = auth_service.create_user("testuser", "test@example.com", "password123")
        api_key = auth_service.create_api_key(user.user_id, "test-key")
        assert api_key is not None
        assert api_key.startswith("bdc_")

        verified_user = auth_service.verify_api_key(api_key)
        assert verified_user is not None
        assert verified_user.username == "testuser"

    def test_verify_invalid_api_key(self, auth_service):
        """Test invalid API key verification fails."""
        user = auth_service.verify_api_key("invalid_key")
        assert user is None

    def test_get_user(self, auth_service):
        """Test get user by ID."""
        created = auth_service.create_user("testuser", "test@example.com", "password123")
        user = auth_service.get_user(created.user_id)
        assert user is not None
        assert user.username == "testuser"

    def test_list_users(self, auth_service):
        """Test list all users."""
        auth_service.create_user("user1", "user1@test.com", "pass123")
        auth_service.create_user("user2", "user2@test.com", "pass123")

        users = auth_service.list_users()
        assert len(users) == 2

    def test_deactivate_user(self, auth_service):
        """Test user deactivation."""
        user = auth_service.create_user("testuser", "test@example.com", "password123")
        result = auth_service.deactivate_user(user.user_id)
        assert result is True

        # Cannot authenticate deactivated user
        auth_user = auth_service.authenticate("testuser", "password123")
        assert auth_user is None

    def test_change_password(self, auth_service):
        """Test password change."""
        user = auth_service.create_user("testuser", "test@example.com", "oldpassword")
        result = auth_service.change_password(user.user_id, "newpassword")
        assert result is True

        # Old password should fail
        auth_user = auth_service.authenticate("testuser", "oldpassword")
        assert auth_user is None

        # New password should work
        auth_user = auth_service.authenticate("testuser", "newpassword")
        assert auth_user is not None


class TestPasswordHashing:
    """Tests for password hashing."""

    def test_password_hash_is_unique(self, auth_service):
        """Test that same password produces different hashes."""
        hash1 = auth_service._hash_password("password123")
        hash2 = auth_service._hash_password("password123")
        assert hash1 != hash2  # Different salts

    def test_password_verification(self, auth_service):
        """Test password verification."""
        password = "testpassword123"
        hash_val = auth_service._hash_password(password)
        assert auth_service._verify_password(password, hash_val) is True
        assert auth_service._verify_password("wrongpassword", hash_val) is False
