# utils/validators.py
"""
Validation utilities for input validation across the application
"""

import re
from typing import Optional


class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass


def validate_sql_query(query: str) -> bool:
    """
    Validate that a SQL query is safe for execution
    Only allows SELECT queries, blocks dangerous operations

    Args:
        query: SQL query string to validate

    Returns:
        bool: True if query is safe

    Raises:
        ValidationError: If query contains dangerous operations
    """
    if not query or not isinstance(query, str):
        raise ValidationError("Query must be a non-empty string")

    query_upper = query.upper().strip()

    # Block dangerous operations
    dangerous_keywords = [
        'DROP', 'DELETE', 'TRUNCATE', 'ALTER',
        'CREATE', 'INSERT', 'UPDATE', 'GRANT',
        'REVOKE', 'EXECUTE', 'EXEC'
    ]

    for keyword in dangerous_keywords:
        # Use word boundary to avoid false positives like "DROPPED_COLUMN"
        if re.search(rf'\b{keyword}\b', query_upper):
            raise ValidationError(
                f"Query contains forbidden keyword: {keyword}. "
                "Only SELECT queries are allowed."
            )

    # Only allow SELECT queries
    if not query_upper.startswith('SELECT'):
        raise ValidationError(
            "Query must start with SELECT. "
            "Only read-only SELECT queries are allowed."
        )

    return True


def validate_question(question: str, max_length: int = 1000) -> bool:
    """
    Validate a question string for RAG queries

    Args:
        question: Question text to validate
        max_length: Maximum allowed length

    Returns:
        bool: True if valid

    Raises:
        ValidationError: If validation fails
    """
    if not question or not isinstance(question, str):
        raise ValidationError("Question must be a non-empty string")

    question_stripped = question.strip()

    if len(question_stripped) == 0:
        raise ValidationError("Question cannot be empty or whitespace only")

    if len(question) > max_length:
        raise ValidationError(
            f"Question too long ({len(question)} characters). "
            f"Maximum allowed: {max_length} characters"
        )

    return True


def validate_file_path(file_path: str, allowed_extensions: Optional[list] = None) -> bool:
    """
    Validate a file path

    Args:
        file_path: Path to file
        allowed_extensions: List of allowed extensions (e.g., ['.pdf', '.txt'])

    Returns:
        bool: True if valid

    Raises:
        ValidationError: If validation fails
    """
    if not file_path or not isinstance(file_path, str):
        raise ValidationError("File path must be a non-empty string")

    # Check for path traversal attempts
    if '..' in file_path or file_path.startswith('/etc') or file_path.startswith('/root'):
        raise ValidationError("Invalid file path: potential security risk")

    # Check file extension if specified
    if allowed_extensions:
        file_lower = file_path.lower()
        if not any(file_lower.endswith(ext) for ext in allowed_extensions):
            raise ValidationError(
                f"File extension not allowed. Allowed: {', '.join(allowed_extensions)}"
            )

    return True


def validate_document_id(document_id: str) -> bool:
    """
    Validate document ID format

    Args:
        document_id: Document ID to validate

    Returns:
        bool: True if valid

    Raises:
        ValidationError: If validation fails
    """
    if not document_id or not isinstance(document_id, str):
        raise ValidationError("Document ID must be a non-empty string")

    document_id = document_id.strip()

    if len(document_id) == 0:
        raise ValidationError("Document ID cannot be empty")

    # Allow alphanumeric, hyphens, underscores, and dots
    if not re.match(r'^[\w\-\.]+$', document_id):
        raise ValidationError(
            "Document ID can only contain letters, numbers, hyphens, underscores, and dots"
        )

    return True


def validate_limit(limit: int, max_limit: int = 1000) -> bool:
    """
    Validate a limit parameter for database queries

    Args:
        limit: Limit value to validate
        max_limit: Maximum allowed limit

    Returns:
        bool: True if valid

    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(limit, int):
        raise ValidationError("Limit must be an integer")

    if limit < 1:
        raise ValidationError("Limit must be at least 1")

    if limit > max_limit:
        raise ValidationError(f"Limit exceeds maximum allowed value of {max_limit}")

    return True