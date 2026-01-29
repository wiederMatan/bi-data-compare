# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly:

1. **Do NOT** create a public GitHub issue for security vulnerabilities
2. Email the security issue to the repository maintainers
3. Include a detailed description of the vulnerability
4. Provide steps to reproduce the issue if possible
5. Allow reasonable time for the issue to be addressed before public disclosure

## Security Best Practices for Users

### Environment Configuration

1. **Never commit sensitive credentials**
   - Use environment variables for all passwords and secrets
   - Copy `.env.example` to `.env` and fill in your values
   - The `.env` file is gitignored and should never be committed

2. **JWT Secret Key**
   - Set `JWT_SECRET_KEY` environment variable for production
   - Generate a strong key: `python -c "import secrets; print(secrets.token_hex(32))"`
   - If not set, tokens won't persist across application restarts

3. **Database SSL Certificates**
   - Set `DB_TRUST_SERVER_CERTIFICATE=no` in production
   - Use proper SSL certificates for database connections
   - Default is `yes` for development convenience only

4. **CORS Configuration**
   - Set `CORS_ALLOWED_ORIGINS` to your specific domains in production
   - Never use wildcard origins in production

### Docker Security

1. **SQL Server Password**
   - Set `SA_PASSWORD` environment variable before running `docker compose`
   - Use strong passwords meeting SQL Server complexity requirements
   - Example: `export SA_PASSWORD='YourSecureP@ssw0rd!' && docker compose up`

2. **Network Security**
   - Don't expose database ports to public networks
   - Use internal Docker networks for service communication

### API Security

1. **Authentication**
   - All API endpoints require authentication
   - Use JWT tokens with proper expiration
   - Store tokens securely on client side

2. **Input Validation**
   - All SQL identifiers are validated before use
   - Parameterized queries are used where possible
   - User inputs are sanitized to prevent injection attacks

## Security Features

### SQL Injection Prevention
- All schema, table, and column names are validated against a strict pattern
- Dangerous SQL keywords are blocked in identifiers
- Parameterized queries used for dynamic values

### XSS Prevention
- HTML output is escaped to prevent cross-site scripting
- User-provided data is sanitized before rendering

### Path Traversal Prevention
- Export file paths are validated to prevent directory traversal
- Only allowed file extensions are permitted for exports

### Sensitive Data Protection
- Passwords are never stored in cache files
- SQL queries are truncated in logs to prevent data exposure
- Credentials loaded only from environment variables

### Authentication
- JWT-based authentication for API endpoints
- Configurable token expiration
- Secure password hashing with bcrypt

## Dependency Security

Keep dependencies updated to address security vulnerabilities:

```bash
# Check for known vulnerabilities
pip install safety
safety check

# Update dependencies
pip install --upgrade -r requirements.txt
```

## Code Security Scanning

This repository includes:
- CodeQL analysis for security vulnerabilities
- Dependency vulnerability scanning
- Pre-commit hooks for security checks

## Contact

For security concerns, contact the repository maintainers through GitHub.
