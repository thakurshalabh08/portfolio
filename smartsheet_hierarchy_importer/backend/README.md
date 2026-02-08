# Smartsheet OAuth2 Authentication Server

This Flask application provides OAuth2 authentication endpoints for Smartsheet API integration.

## Features

- **GET /auth/login** - Redirects user to Smartsheet OAuth page
- **GET /auth/callback** - Handles OAuth callback and exchanges code for tokens
- **GET /auth/me** - Returns authenticated user's Smartsheet profile info
- **POST /auth/logout** - Logs out user and clears tokens
- **GET /auth/status** - Checks authentication status
- **GET /health** - Health check endpoint

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy the configuration template and fill in your values:

```bash
cp config_template.env .env
```

Edit `.env` with your actual Smartsheet OAuth2 credentials:

```env
SMARTSHEET_CLIENT_ID=your_actual_client_id
SMARTSHEET_CLIENT_SECRET=your_actual_client_secret
SMARTSHEET_REDIRECT_URI=http://localhost:5000/auth/callback
SECRET_KEY=your_random_secret_key
```

### 3. Smartsheet App Registration

1. Go to [Smartsheet Developer Portal](https://app.smartsheet.com/b/home?portal=devportal)
2. Create a new app
3. Set the redirect URI to: `http://localhost:5000/auth/callback`
4. Copy the Client ID and Client Secret to your `.env` file

### 4. Run the Server

```bash
python auth_server.py
```

The server will start on `http://localhost:5000`

## Usage

### Authentication Flow

1. **Start Authentication**: Visit `http://localhost:5000/auth/login`
   - This redirects to Smartsheet OAuth page
   - User logs in and authorizes your app

2. **OAuth Callback**: Smartsheet redirects to `/auth/callback`
   - Server exchanges authorization code for access/refresh tokens
   - Tokens are stored securely
   - User is redirected to dashboard

3. **Get User Profile**: Call `GET /auth/me`
   - Returns authenticated user's profile information
   - Requires valid session

### API Endpoints

#### GET /auth/login
Initiates OAuth2 flow by redirecting to Smartsheet.

**Response**: Redirect to Smartsheet OAuth page

#### GET /auth/callback
Handles OAuth2 callback from Smartsheet.

**Query Parameters**:
- `code` - Authorization code from Smartsheet
- `state` - State parameter for security

**Response**: Redirect to dashboard or error

#### GET /auth/me
Returns authenticated user's profile information.

**Headers**: Requires valid session

**Response**:
```json
{
  "id": "user_id",
  "email": "user@example.com",
  "firstName": "John",
  "lastName": "Doe",
  "locale": "en_US",
  "timeZone": "America/New_York",
  "account": {
    "id": "account_id",
    "name": "Account Name"
  }
}
```

#### POST /auth/logout
Logs out user and clears all tokens.

**Response**:
```json
{
  "message": "Logged out successfully"
}
```

#### GET /auth/status
Checks if user is currently authenticated.

**Response**:
```json
{
  "authenticated": true,
  "user_id": "user_id",
  "email": "user@example.com"
}
```

#### GET /health
Health check endpoint.

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00"
}
```

## Security Features

- **State Parameter**: Prevents CSRF attacks during OAuth flow
- **Token Storage**: Secure in-memory token storage (customize for production)
- **Token Refresh**: Automatic access token refresh using refresh tokens
- **Session Management**: Secure session handling with Flask sessions
- **Error Handling**: Comprehensive error handling and logging

## Production Considerations

### Token Storage
The current implementation uses in-memory storage for demo purposes. For production:

1. **Database Storage**: Use PostgreSQL or MySQL for persistent token storage
2. **Redis**: Use Redis for high-performance token caching
3. **Encryption**: Encrypt tokens before storing in database

### Security Enhancements
1. **HTTPS**: Always use HTTPS in production
2. **Secure Cookies**: Configure secure session cookies
3. **Token Rotation**: Implement token rotation policies
4. **Rate Limiting**: Add rate limiting to prevent abuse

### Example Production Token Storage

```python
import redis
import json
from cryptography.fernet import Fernet

class RedisTokenStorage:
    def __init__(self, redis_url):
        self.redis_client = redis.from_url(redis_url)
        self.cipher = Fernet(os.environ.get('ENCRYPTION_KEY'))
    
    def store_tokens(self, user_id, access_token, refresh_token, expires_in):
        # Encrypt tokens before storing
        encrypted_data = self.cipher.encrypt(json.dumps({
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_at': (datetime.now() + timedelta(seconds=expires_in)).isoformat()
        }).encode())
        
        self.redis_client.setex(f"tokens:{user_id}", expires_in, encrypted_data)
```

## Error Handling

The application includes comprehensive error handling:

- **OAuth Errors**: Handles Smartsheet OAuth errors gracefully
- **Token Errors**: Manages token expiration and refresh failures
- **Network Errors**: Handles API communication failures
- **Validation Errors**: Validates all input parameters

## Logging

All operations are logged with appropriate levels:
- **INFO**: Successful operations
- **ERROR**: Failed operations and errors
- **DEBUG**: Detailed debugging information (in development mode)

## Testing

Test the authentication flow:

1. Start the server: `python auth_server.py`
2. Visit: `http://localhost:5000/auth/login`
3. Complete Smartsheet OAuth flow
4. Check user profile: `http://localhost:5000/auth/me`
5. Test logout: `POST http://localhost:5000/auth/logout`
