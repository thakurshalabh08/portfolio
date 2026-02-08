#!/usr/bin/env python3
"""
Test script for Smartsheet OAuth2 Authentication Server
This script demonstrates how to interact with the authentication endpoints
"""

import requests
import json
import time
from urllib.parse import urlparse, parse_qs

# Configuration
BASE_URL = "http://localhost:5000"
TEST_EMAIL = "test@example.com"  # Replace with your test email

def test_health_check():
    """Test the health check endpoint"""
    print("Testing health check...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            print("✅ Health check passed")
            print(f"Response: {response.json()}")
        else:
            print(f"❌ Health check failed: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Health check failed: {e}")

def test_auth_status():
    """Test authentication status"""
    print("\nTesting auth status...")
    try:
        response = requests.get(f"{BASE_URL}/auth/status")
        if response.status_code == 200:
            data = response.json()
            print("✅ Auth status check passed")
            print(f"Authenticated: {data.get('authenticated', False)}")
            if data.get('authenticated'):
                print(f"User ID: {data.get('user_id')}")
                print(f"Email: {data.get('email')}")
        else:
            print(f"❌ Auth status check failed: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Auth status check failed: {e}")

def test_auth_login():
    """Test the login endpoint (will redirect to Smartsheet)"""
    print("\nTesting auth login...")
    try:
        # Don't follow redirects to see the redirect URL
        response = requests.get(f"{BASE_URL}/auth/login", allow_redirects=False)
        if response.status_code == 302:
            print("✅ Auth login redirect successful")
            redirect_url = response.headers.get('Location', '')
            print(f"Redirect URL: {redirect_url}")
            
            # Parse the redirect URL to check OAuth parameters
            parsed_url = urlparse(redirect_url)
            query_params = parse_qs(parsed_url.query)
            
            print("OAuth Parameters:")
            print(f"  - Client ID: {query_params.get('client_id', ['Not found'])[0]}")
            print(f"  - Response Type: {query_params.get('response_type', ['Not found'])[0]}")
            print(f"  - Redirect URI: {query_params.get('redirect_uri', ['Not found'])[0]}")
            print(f"  - Scope: {query_params.get('scope', ['Not found'])[0]}")
            print(f"  - State: {query_params.get('state', ['Not found'])[0][:20]}...")
            
            return redirect_url
        else:
            print(f"❌ Auth login failed: {response.status_code}")
            print(f"Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Auth login failed: {e}")
    return None

def test_auth_me():
    """Test the /auth/me endpoint (requires authentication)"""
    print("\nTesting auth/me endpoint...")
    try:
        response = requests.get(f"{BASE_URL}/auth/me")
        if response.status_code == 200:
            print("✅ Auth/me endpoint successful")
            user_data = response.json()
            print("User Profile:")
            for key, value in user_data.items():
                print(f"  - {key}: {value}")
        elif response.status_code == 401:
            print("⚠️  Auth/me endpoint requires authentication (expected)")
            print("Complete OAuth flow first to test this endpoint")
        else:
            print(f"❌ Auth/me endpoint failed: {response.status_code}")
            print(f"Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Auth/me endpoint failed: {e}")

def test_auth_logout():
    """Test the logout endpoint"""
    print("\nTesting auth logout...")
    try:
        response = requests.post(f"{BASE_URL}/auth/logout")
        if response.status_code == 200:
            print("✅ Auth logout successful")
            print(f"Response: {response.json()}")
        else:
            print(f"❌ Auth logout failed: {response.status_code}")
            print(f"Response: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Auth logout failed: {e}")

def interactive_oauth_test():
    """Interactive test for OAuth flow"""
    print("\n" + "="*60)
    print("INTERACTIVE OAUTH TEST")
    print("="*60)
    print("This test will guide you through the OAuth flow manually.")
    print("Make sure the auth server is running on http://localhost:5000")
    print()
    
    # Test login redirect
    redirect_url = test_auth_login()
    if not redirect_url:
        print("❌ Cannot proceed with OAuth test - login redirect failed")
        return
    
    print("\n📋 MANUAL OAUTH STEPS:")
    print("1. Open the redirect URL above in your browser")
    print("2. Log in to Smartsheet and authorize the application")
    print("3. You will be redirected back to the callback URL")
    print("4. After successful authentication, run the test again")
    print()
    
    input("Press Enter after completing the OAuth flow...")
    
    # Test authenticated endpoints
    test_auth_status()
    test_auth_me()

def main():
    """Main test function"""
    print("Smartsheet OAuth2 Authentication Server - Test Suite")
    print("="*60)
    
    # Basic endpoint tests
    test_health_check()
    test_auth_status()
    test_auth_login()
    test_auth_me()
    test_auth_logout()
    
    # Interactive OAuth test
    print("\n" + "="*60)
    print("Would you like to run the interactive OAuth test?")
    print("This will test the complete authentication flow.")
    choice = input("Enter 'y' to continue, or any other key to exit: ").lower().strip()
    
    if choice == 'y':
        interactive_oauth_test()
    
    print("\n" + "="*60)
    print("Test suite completed!")
    print("="*60)

if __name__ == "__main__":
    main()
