# services/database.py
"""
Database service - Pure business logic, no MCP knowledge
Can be used by MCP, REST API, CLI, or any other interface
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional

from utils.validators import validate_sql_query, validate_limit, ValidationError

logger = logging.getLogger(__name__)


class DatabaseService:
    """Manages PostgreSQL connections and business operations"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def execute_select_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query safely
        Returns: List of dictionaries (rows)
        Raises: ValidationError if query is not SELECT
        """
        # Validate query using utility
        validate_sql_query(query)

        try:
            if not self.connection or self.connection.closed:
                self.connect()

            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"Query executed: {query[:100]}... | Rows: {len(results)}")
                return results
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def get_documents(self, limit: int = 10, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get documents from database with optional filtering
        Business logic method - knows about document structure
        """
        # Validate limit using utility
        validate_limit(limit)

        if user_id:
            query = "SELECT * FROM documents WHERE user_id = %s LIMIT %s"
            params = (user_id, limit)
        else:
            query = "SELECT * FROM documents LIMIT %s"
            params = (limit,)

        return self.execute_select_query(query, params)

    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get user information by ID
        Returns: User dict or None if not found
        """
        query = "SELECT * FROM users WHERE id = %s"
        results = self.execute_select_query(query, (user_id,))

        return results[0] if results else None

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")