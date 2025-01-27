import bcrypt
import jwt
from datetime import datetime, timedelta
from fastapi import HTTPException
import duckdb
import os

# Constants for JWT
SECRET_KEY = "55555"
ALGORITHM = "HS256"
TOKEN_EXPIRATION_MINUTES = 30  # Expiry time for JWT tokens

# Connect to MotherDuckDB or fallback to local DuckDB
try:
    conn = duckdb.connect("md:nba_data?motherduck_token={}".format(os.getenv('motherduck_token')))
except Exception as e:
    raise RuntimeError(f"Failed to connect to the database: {str(e)}")


def initialize_user_table():
    """
    Creates the `app_users` table if it does not already exist.
    """
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_users (
                id INTEGER PRIMARY KEY,    -- Manually generated unique ID
                full_name TEXT NOT NULL,   -- Full name of the user
                username TEXT NOT NULL UNIQUE,   -- Unique username
                hashed_password TEXT NOT NULL   -- Hashed password
            );
        """)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize user table: {str(e)}")


def register_user(full_name: str, username: str, password: str):
    """
    Registers a new user in the database.

    Args:
        full_name (str): The full name of the user.
        username (str): The username of the user.
        password (str): The plaintext password to be hashed.

    Returns:
        dict: A success message.

    Raises:
        HTTPException: If the username already exists or registration fails.
    """
    try:
        # Ensure the table exists
        initialize_user_table()

        # Check if the username already exists
        user_exists = conn.execute("SELECT COUNT(*) FROM app_users WHERE username = ?", (username,)).fetchone()[0]
        if user_exists > 0:
            raise HTTPException(status_code=400, detail="Username already exists")

        # Hash the password
        hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

        # Get the next unique ID
        next_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM app_users").fetchone()[0]

        # Insert the user into the database
        conn.execute(
            "INSERT INTO app_users (id, full_name, username, hashed_password) VALUES (?, ?, ?, ?)",
            (next_id, full_name, username, hashed_password)
        )
        return {"message": "User registered successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register user: {str(e)}")


def login_user(username: str, password: str):
    """
    Authenticates a user and generates a JWT.

    Args:
        username (str): The username of the user.
        password (str): The plaintext password to verify.

    Returns:
        dict: The JWT token and token type.

    Raises:
        HTTPException: If authentication fails.
    """
    try:
        # Retrieve the hashed password for the given username
        result = conn.execute("SELECT hashed_password FROM app_users WHERE username = ?", (username,)).fetchone()
        if not result:
            raise HTTPException(status_code=400, detail="Invalid username or password")

        hashed_password = result[0]
        # Verify the provided password against the stored hash
        if not bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8")):
            raise HTTPException(status_code=400, detail="Invalid username or password")

        # Generate a JWT token
        token = jwt.encode(
            {"sub": username, "exp": datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRATION_MINUTES)},
            SECRET_KEY,
            algorithm=ALGORITHM
        )
        return {"access_token": token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to login: {str(e)}")


def verify_token(token: str):
    """
    Verifies a JWT and returns the username.

    Args:
        token (str): The JWT token to verify.

    Returns:
        str: The username associated with the token.

    Raises:
        HTTPException: If the token is invalid or expired.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload["sub"]
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
