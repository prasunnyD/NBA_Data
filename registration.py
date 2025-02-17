import bcrypt
import jwt
from datetime import datetime, timedelta
from fastapi import HTTPException
import duckdb
import os

MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")


# Securely fetch environment variables
SECRET_KEY = os.getenv("SECRET_KEY")

# Constants for JWT
ALGORITHM = "HS256"
TOKEN_EXPIRATION_MINUTES = 30  # Expiry time for JWT tokens

class UserRegistration:
    """
    Class to handle user registration, authentication, and token verification.
    """

    def __init__(self):
        self.db_path = f"md:nba_data?motherduck_token={MOTHERDUCK_TOKEN}"

    def _connect_db(self):
        """Creates a new database connection."""
        return duckdb.connect(self.db_path)

    def initialize_user_table(self):
        """Creates the `app_users` table if it does not already exist."""
        try:
            with self._connect_db() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS app_users (
                        id INTEGER PRIMARY KEY,  
                        full_name TEXT NOT NULL,
                        username TEXT NOT NULL UNIQUE,
                        hashed_password TEXT NOT NULL
                    );
                """)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize user table: {str(e)}")

    def register_user(self, full_name: str, username: str, password: str):
        """
        Registers a new user in the database.

        Args:
            full_name (str): The full name of the user.
            username (str): The username.
            password (str): The plaintext password.

        Returns:
            dict: Success message.
        """
        try:
            self.initialize_user_table()  # Ensure table exists

            with self._connect_db() as conn:
                # Check if the username already exists
                user_exists = conn.execute(
                    "SELECT COUNT(*) FROM app_users WHERE username = ?", 
                    (username,)
                ).fetchone()[0]

                if user_exists > 0:
                    raise HTTPException(status_code=400, detail="Username already exists")

                # Hash the password
                hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

                # Get the next unique ID
                next_id = conn.execute(
                    "SELECT COALESCE(MAX(id), 0) + 1 FROM app_users"
                ).fetchone()[0]

                # Insert new user
                conn.execute(
                    "INSERT INTO app_users (id, full_name, username, hashed_password) VALUES (?, ?, ?, ?)",
                    (next_id, full_name, username, hashed_password)
                )

            return {"message": "User registered successfully"}

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to register user: {str(e)}")

    def login_user(self, username: str, password: str):
        """
        Authenticates a user and generates a JWT token.

        Args:
            username (str): The username.
            password (str): The plaintext password.

        Returns:
            dict: JWT token and token type.
        """
        try:
            with self._connect_db() as conn:
                result = conn.execute(
                    "SELECT hashed_password FROM app_users WHERE username = ?", 
                    (username,)
                ).fetchone()

            if not result:
                raise HTTPException(status_code=400, detail="Invalid username or password")

            hashed_password = result[0]

            # Ensure hashed_password is a string
            if isinstance(hashed_password, bytes):
                hashed_password = hashed_password.decode("utf-8")

            if not bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8")):
                raise HTTPException(status_code=400, detail="Invalid username or password")

            # Generate JWT Token
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

    def verify_token(self, token: str):
        """
        Verifies a JWT and returns the associated username.

        Args:
            token (str): The JWT token.

        Returns:
            str: The username associated with the token.
        """
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return payload["sub"]
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")