# tools/db_tools.py
"""
Database MCP tools - MCP-specific wrappers around DatabaseService
Handles MCP schemas, formatting, and error translation
"""

import logging
from typing import List
from mcp.types import Tool, TextContent
from services.database import DatabaseService
from utils.formatters import format_database_results, format_error_message
from utils.validators import ValidationError

logger = logging.getLogger(__name__)


class DatabaseTools:
    """MCP tools for database operations"""

    def __init__(self, db_service: DatabaseService):
        self.db = db_service

    def get_tool_definitions(self) -> List[Tool]:
        """Return MCP tool schemas for database operations"""
        return [
            Tool(
                name="query_database",
                description="Execute a READ-ONLY SQL SELECT query on the PostgreSQL database. Only SELECT queries are allowed.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL SELECT query to execute. Must start with SELECT. No DROP/DELETE/ALTER allowed."
                        }
                    },
                    "required": ["query"]
                }
            ),
            Tool(
                name="get_documents_from_db",
                description="Get a list of documents from the database with filtering options",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of documents to return (default: 10)"
                        },
                        "user_id": {
                            "type": "string",
                            "description": "Optional: Filter by user ID"
                        }
                    }
                }
            ),
            Tool(
                name="get_user_info",
                description="Get information about a specific user from the database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "user_id": {
                            "type": "string",
                            "description": "The user ID to look up"
                        }
                    },
                    "required": ["user_id"]
                }
            )
        ]

    async def execute_tool(self, name: str, arguments: dict) -> List[TextContent]:
        """
        Execute a database tool
        This is the MCP-specific execution logic that:
        1. Calls the service layer
        2. Formats results for MCP
        3. Handles MCP-specific error formatting
        """
        try:
            if name == "query_database":
                return await self._handle_query_database(arguments)

            elif name == "get_documents_from_db":
                return await self._handle_get_documents(arguments)

            elif name == "get_user_info":
                return await self._handle_get_user_info(arguments)

            else:
                raise ValueError(f"Unknown database tool: {name}")

        except Exception as e:
            error_message = format_error_message(e, name)
            logger.error(error_message)
            return [TextContent(type="text", text=error_message)]

    async def _handle_query_database(self, arguments: dict) -> List[TextContent]:
        """Handle custom SQL query execution"""
        query = arguments["query"]

        # Call service layer
        results = self.db.execute_select_query(query)

        # Format for MCP using utility
        result_text = f"Query executed successfully. Found {len(results)} rows.\n\n"
        result_text += format_database_results(results)

        return [TextContent(type="text", text=result_text)]

    async def _handle_get_documents(self, arguments: dict) -> List[TextContent]:
        """Handle get documents request"""
        limit = arguments.get("limit", 10)
        user_id = arguments.get("user_id")

        # Call service layer
        results = self.db.get_documents(limit=limit, user_id=user_id)

        # Format for MCP using utility
        result_text = f"Found {len(results)} documents:\n\n"
        result_text += format_database_results(results)

        return [TextContent(type="text", text=result_text)]

    async def _handle_get_user_info(self, arguments: dict) -> List[TextContent]:
        """Handle get user info request"""
        user_id = arguments["user_id"]

        # Call service layer
        user = self.db.get_user(user_id)

        # Format for MCP using utility
        if not user:
            result_text = f"User not found: {user_id}"
        else:
            result_text = "User information:\n\n"
            result_text += format_database_results([user])

        return [TextContent(type="text", text=result_text)]