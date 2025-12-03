# utils/formatters.py
"""
Formatting utilities for converting data to human-readable text
"""

from typing import List, Dict, Any, Optional
from datetime import datetime


def format_database_results(results: List[Dict[str, Any]], max_records: Optional[int] = None) -> str:
    """
    Format database query results as readable text

    Args:
        results: List of dictionaries (rows from database)
        max_records: Optional limit on number of records to format

    Returns:
        str: Formatted text representation
    """
    if not results:
        return "No results found"

    # Limit records if specified
    display_results = results[:max_records] if max_records else results
    truncated = len(results) > len(display_results) if max_records else False

    output = []
    for i, row in enumerate(display_results, 1):
        output.append(f"\n--- Record {i} ---")
        for key, value in row.items():
            # Format special types
            formatted_value = _format_value(value)
            output.append(f"{key}: {formatted_value}")

    result_text = '\n'.join(output)

    # Add truncation notice if applicable
    if truncated:
        result_text += f"\n\n(Showing {len(display_results)} of {len(results)} total records)"

    return result_text


def format_rag_response(response: Dict[str, Any]) -> str:
    """
    Format RAG API response for display

    Args:
        response: Response dict from RAG API with 'answer' and 'context' keys

    Returns:
        str: Formatted response text
    """
    answer = response.get("answer", "No answer provided")
    context = response.get("context", [])

    result_text = f"Answer: {answer}\n\n"

    if context:
        result_text += "Sources:\n"
        for i, ctx in enumerate(context, 1):
            result_text += _format_context_item(ctx, i)

    return result_text


def format_document_list(documents: List[Dict[str, Any]]) -> str:
    """
    Format a list of documents for display

    Args:
        documents: List of document dictionaries

    Returns:
        str: Formatted document list
    """
    if not documents:
        return "No documents found"

    result_text = f"Found {len(documents)} document(s):\n\n"

    for i, doc in enumerate(documents, 1):
        doc_id = doc.get("id", "unknown")
        title = doc.get("title", "Untitled")
        created = doc.get("created_at", "unknown")

        # Format timestamp if it's a datetime object
        if isinstance(created, datetime):
            created = created.strftime("%Y-%m-%d %H:%M:%S")

        result_text += f"{i}. {title}\n"
        result_text += f"   ID: {doc_id}\n"
        result_text += f"   Created: {created}\n"

        # Add optional fields if present
        if "description" in doc and doc["description"]:
            result_text += f"   Description: {doc['description']}\n"
        if "page_count" in doc:
            result_text += f"   Pages: {doc['page_count']}\n"

        result_text += "\n"

    return result_text


def format_upload_result(response: Dict[str, Any], file_path: str) -> str:
    """
    Format PDF upload result for display

    Args:
        response: Response dict from upload API
        file_path: Path to uploaded file

    Returns:
        str: Formatted upload result
    """
    doc_id = response.get("document_id", "unknown")
    status = response.get("status", "unknown")

    result_text = f"PDF uploaded successfully!\n"
    result_text += f"File: {file_path}\n"
    result_text += f"Document ID: {doc_id}\n"
    result_text += f"Status: {status}\n"

    # Add optional metadata if present
    if "title" in response:
        result_text += f"Title: {response['title']}\n"
    if "chunks_created" in response:
        result_text += f"Chunks created: {response['chunks_created']}\n"

    return result_text


def format_error_message(error: Exception, tool_name: str) -> str:
    """
    Format error message for consistent error reporting

    Args:
        error: Exception that occurred
        tool_name: Name of the tool that failed

    Returns:
        str: Formatted error message
    """
    error_type = type(error).__name__
    error_msg = str(error)

    return f"Error executing {tool_name}: [{error_type}] {error_msg}"


def _format_value(value: Any) -> str:
    """Format a single value for display"""
    if value is None:
        return "NULL"
    elif isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(value, (list, dict)):
        return str(value)
    else:
        return str(value)


def _format_context_item(ctx: Dict[str, Any], index: int) -> str:
    """Format a single context item from RAG response"""
    page = ctx.get("page", "unknown")
    similarity = ctx.get("similarity", 0)
    text = ctx.get("text", "")

    # Truncate long text
    text_preview = text[:150] + "..." if len(text) > 150 else text

    output = f"\n{index}. Page {page}"

    if similarity:
        output += f" (relevance: {similarity:.2%})"

    output += f"\n   {text_preview}\n"

    return output


def format_table(data: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> str:
    """
    Format data as a simple ASCII table

    Args:
        data: List of dictionaries to format
        columns: Optional list of column names to display (default: all)

    Returns:
        str: Formatted table
    """
    if not data:
        return "No data to display"

    # Determine columns
    if columns is None:
        columns = list(data[0].keys())

    # Calculate column widths
    col_widths = {col: len(col) for col in columns}
    for row in data:
        for col in columns:
            value_str = str(row.get(col, ""))
            col_widths[col] = max(col_widths[col], len(value_str))

    # Build table
    output = []

    # Header
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    output.append(header)
    output.append("-" * len(header))

    # Rows
    for row in data:
        row_str = " | ".join(
            str(row.get(col, "")).ljust(col_widths[col])
            for col in columns
        )
        output.append(row_str)

    return "\n".join(output)