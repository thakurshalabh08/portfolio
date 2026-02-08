"""
Smartsheet OAuth2 Authentication Server
Handles authentication flow for Smartsheet API integration
"""

import os
import secrets
import requests
from flask import Flask, request, redirect, session, jsonify, url_for
from urllib.parse import urlencode, parse_qs
import json
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(32))

# Smartsheet OAuth2 Configuration
SMARTSHEET_CLIENT_ID = os.environ.get('SMARTSHEET_CLIENT_ID')
SMARTSHEET_CLIENT_SECRET = os.environ.get('SMARTSHEET_CLIENT_SECRET')
SMARTSHEET_REDIRECT_URI = os.environ.get('SMARTSHEET_REDIRECT_URI', 'http://localhost:5000/auth/callback')
SMARTSHEET_AUTH_URL = 'https://app.smartsheet.com/b/authorize'
SMARTSHEET_TOKEN_URL = 'https://api.smartsheet.com/2.0/token'
SMARTSHEET_USER_INFO_URL = 'https://api.smartsheet.com/2.0/users/me'

# Validate required environment variables
if not SMARTSHEET_CLIENT_ID or not SMARTSHEET_CLIENT_SECRET:
    logger.error("Missing required environment variables: SMARTSHEET_CLIENT_ID and SMARTSHEET_CLIENT_SECRET")
    raise ValueError("Missing required OAuth2 credentials")

class TokenStorage:
    """Simple in-memory token storage for demo purposes.
    In production, use a proper database like Redis or PostgreSQL."""
    
    def __init__(self):
        self.tokens = {}
    
    def store_tokens(self, user_id, access_token, refresh_token, expires_in):
        """Store tokens for a user"""
        expires_at = datetime.now() + timedelta(seconds=expires_in)
        self.tokens[user_id] = {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_at': expires_at,
            'created_at': datetime.now()
        }
        logger.info(f"Stored tokens for user: {user_id}")
    
    def get_tokens(self, user_id):
        """Get tokens for a user"""
        return self.tokens.get(user_id)
    
    def refresh_access_token(self, user_id, new_access_token, expires_in):
        """Update access token after refresh"""
        if user_id in self.tokens:
            expires_at = datetime.now() + timedelta(seconds=expires_in)
            self.tokens[user_id]['access_token'] = new_access_token
            self.tokens[user_id]['expires_at'] = expires_at
            self.tokens[user_id]['updated_at'] = datetime.now()
            logger.info(f"Refreshed access token for user: {user_id}")
            return True
        return False
    
    def remove_tokens(self, user_id):
        """Remove tokens for a user (logout)"""
        if user_id in self.tokens:
            del self.tokens[user_id]
            logger.info(f"Removed tokens for user: {user_id}")

# Initialize token storage
token_storage = TokenStorage()

def generate_state():
    """Generate a random state parameter for OAuth2 flow"""
    return secrets.token_urlsafe(32)

def validate_state(state):
    """Validate the state parameter"""
    return state and len(state) > 10

def exchange_code_for_tokens(auth_code):
    """Exchange authorization code for access and refresh tokens"""
    try:
        token_data = {
            'grant_type': 'authorization_code',
            'client_id': SMARTSHEET_CLIENT_ID,
            'client_secret': SMARTSHEET_CLIENT_SECRET,
            'code': auth_code,
            'redirect_uri': SMARTSHEET_REDIRECT_URI
        }
        
        response = requests.post(SMARTSHEET_TOKEN_URL, data=token_data)
        response.raise_for_status()
        
        token_response = response.json()
        logger.info("Successfully exchanged code for tokens")
        
        return {
            'access_token': token_response.get('access_token'),
            'refresh_token': token_response.get('refresh_token'),
            'expires_in': token_response.get('expires_in', 3600),
            'token_type': token_response.get('token_type', 'Bearer')
        }
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error exchanging code for tokens: {e}")
        return None

def refresh_access_token(refresh_token):
    """Refresh access token using refresh token"""
    try:
        token_data = {
            'grant_type': 'refresh_token',
            'client_id': SMARTSHEET_CLIENT_ID,
            'client_secret': SMARTSHEET_CLIENT_SECRET,
            'refresh_token': refresh_token
        }
        
        response = requests.post(SMARTSHEET_TOKEN_URL, data=token_data)
        response.raise_for_status()
        
        token_response = response.json()
        logger.info("Successfully refreshed access token")
        
        return {
            'access_token': token_response.get('access_token'),
            'expires_in': token_response.get('expires_in', 3600),
            'token_type': token_response.get('token_type', 'Bearer')
        }
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error refreshing access token: {e}")
        return None

def get_user_profile(access_token):
    """Get user profile information from Smartsheet API"""
    try:
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(SMARTSHEET_USER_INFO_URL, headers=headers)
        response.raise_for_status()
        
        user_data = response.json()
        logger.info(f"Retrieved user profile for: {user_data.get('email', 'Unknown')}")
        
        return user_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting user profile: {e}")
        return None

def is_token_valid(user_id):
    """Check if user's access token is still valid"""
    tokens = token_storage.get_tokens(user_id)
    if not tokens:
        return False
    
    # Check if token is expired
    if datetime.now() >= tokens['expires_at']:
        # Try to refresh the token
        refresh_result = refresh_access_token(tokens['refresh_token'])
        if refresh_result:
            token_storage.refresh_access_token(
                user_id, 
                refresh_result['access_token'], 
                refresh_result['expires_in']
            )
            return True
        else:
            # Refresh failed, remove tokens
            token_storage.remove_tokens(user_id)
            return False
    
    return True

@app.route('/auth/login', methods=['GET'])
def auth_login():
    """
    GET /auth/login
    Redirects user to Smartsheet OAuth page
    """
    try:
        # Generate state parameter for security
        state = generate_state()
        session['oauth_state'] = state
        
        # Build authorization URL
        auth_params = {
            'response_type': 'code',
            'client_id': SMARTSHEET_CLIENT_ID,
            'redirect_uri': SMARTSHEET_REDIRECT_URI,
            'scope': 'READ_SHEETS,WRITE_SHEETS,ADMIN_USERS',
            'state': state
        }
        
        auth_url = f"{SMARTSHEET_AUTH_URL}?{urlencode(auth_params)}"
        logger.info(f"Redirecting to Smartsheet OAuth: {auth_url}")
        
        return redirect(auth_url)
    
    except Exception as e:
        logger.error(f"Error in auth_login: {e}")
        return jsonify({'error': 'Authentication initiation failed'}), 500

@app.route('/auth/callback', methods=['GET'])
def auth_callback():
    """
    GET /auth/callback
    Smartsheet redirects here with auth code.
    Exchange code for access + refresh token.
    Store token in session or DB.
    Redirect frontend to dashboard.
    """
    try:
        # Get parameters from callback
        auth_code = request.args.get('code')
        state = request.args.get('state')
        error = request.args.get('error')
        
        # Handle OAuth errors
        if error:
            logger.error(f"OAuth error: {error}")
            return jsonify({'error': f'OAuth error: {error}'}), 400
        
        # Validate state parameter
        if not validate_state(state) or state != session.get('oauth_state'):
            logger.error("Invalid state parameter")
            return jsonify({'error': 'Invalid state parameter'}), 400
        
        # Validate authorization code
        if not auth_code:
            logger.error("Missing authorization code")
            return jsonify({'error': 'Missing authorization code'}), 400
        
        # Exchange code for tokens
        token_data = exchange_code_for_tokens(auth_code)
        if not token_data:
            return jsonify({'error': 'Failed to exchange code for tokens'}), 500
        
        # Get user profile
        user_profile = get_user_profile(token_data['access_token'])
        if not user_profile:
            return jsonify({'error': 'Failed to get user profile'}), 500
        
        user_id = user_profile.get('id')
        if not user_id:
            return jsonify({'error': 'Invalid user profile'}), 500
        
        # Store tokens
        token_storage.store_tokens(
            user_id,
            token_data['access_token'],
            token_data['refresh_token'],
            token_data['expires_in']
        )
        
        # Store user info in session
        session['user_id'] = user_id
        session['user_email'] = user_profile.get('email')
        session['authenticated'] = True
        
        logger.info(f"Successfully authenticated user: {user_profile.get('email')}")
        
        # Redirect to dashboard (you can customize this URL)
        dashboard_url = request.args.get('redirect_uri', '/dashboard')
        return redirect(dashboard_url)
    
    except Exception as e:
        logger.error(f"Error in auth_callback: {e}")
        return jsonify({'error': 'Authentication callback failed'}), 500

@app.route('/auth/me', methods=['GET'])
def auth_me():
    """
    GET /auth/me
    Returns authenticated user's Smartsheet profile info
    """
    try:
        # Check if user is authenticated
        if not session.get('authenticated'):
            return jsonify({'error': 'Not authenticated'}), 401
        
        user_id = session.get('user_id')
        if not user_id:
            return jsonify({'error': 'No user ID in session'}), 401
        
        # Check if token is valid
        if not is_token_valid(user_id):
            return jsonify({'error': 'Invalid or expired token'}), 401
        
        # Get fresh user profile
        tokens = token_storage.get_tokens(user_id)
        user_profile = get_user_profile(tokens['access_token'])
        
        if not user_profile:
            return jsonify({'error': 'Failed to get user profile'}), 500
        
        # Return user profile (excluding sensitive data)
        safe_profile = {
            'id': user_profile.get('id'),
            'email': user_profile.get('email'),
            'firstName': user_profile.get('firstName'),
            'lastName': user_profile.get('lastName'),
            'locale': user_profile.get('locale'),
            'timeZone': user_profile.get('timeZone'),
            'account': {
                'id': user_profile.get('account', {}).get('id'),
                'name': user_profile.get('account', {}).get('name')
            } if user_profile.get('account') else None
        }
        
        return jsonify(safe_profile)
    
    except Exception as e:
        logger.error(f"Error in auth_me: {e}")
        return jsonify({'error': 'Failed to get user profile'}), 500

@app.route('/auth/logout', methods=['POST'])
def auth_logout():
    """
    POST /auth/logout
    Logout user and clear tokens
    """
    try:
        user_id = session.get('user_id')
        if user_id:
            token_storage.remove_tokens(user_id)
        
        # Clear session
        session.clear()
        
        logger.info("User logged out successfully")
        return jsonify({'message': 'Logged out successfully'})
    
    except Exception as e:
        logger.error(f"Error in auth_logout: {e}")
        return jsonify({'error': 'Logout failed'}), 500

@app.route('/auth/status', methods=['GET'])
def auth_status():
    """
    GET /auth/status
    Check authentication status
    """
    try:
        if not session.get('authenticated'):
            return jsonify({'authenticated': False})
        
        user_id = session.get('user_id')
        if not user_id or not is_token_valid(user_id):
            return jsonify({'authenticated': False})
        
        return jsonify({
            'authenticated': True,
            'user_id': user_id,
            'email': session.get('user_email')
        })
    
    except Exception as e:
        logger.error(f"Error in auth_status: {e}")
        return jsonify({'authenticated': False})

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    # Development server
    app.run(debug=True, host='0.0.0.0', port=5000)
