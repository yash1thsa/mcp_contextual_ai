#!/usr/bin/env python3
"""
MCP Server - Orchestration layer
Just handles MCP protocol, delegates to tools
"""

import asyncio
import os
import logging
from mcp.server import Server
from mcp.server.stdio import stdio_server

# Import services
from services.database import DatabaseService
from services.rag import RAGService

# Import tools
from tools.db_tools import DatabaseTools
from tools.rag_tools import RAGTools

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL",
                         "postgresql://yash1thsa:81LTYIIV6rFQ2Jj2FIn1PGeOTAomObKH@dpg-d4banuvdiees73af02r0-a.ohio-postgres.render.com/pdf_ingest")
RAG_API_URL = os.getenv("RAG_API_URL", "https://contextual-ai-cw79.onrender.com/")
RAG_API_KEY = ''

# Initialize services (pure business logic, no MCP)
db_service = DatabaseService(DATABASE_URL)
rag_service = RAGService(RAG_API_URL, RAG_API_KEY)

# Initialize tools (MCP wrappers around services)
db_tools = DatabaseTools(db_service)
rag_tools = RAGTools(rag_service)

# Create MCP server
app = Server("rag-db-mcp-server")


@app.list_tools()
async def list_tools():
    """Collect all tool definitions from all tool modules"""
    tools = []

    # Add database tools
    tools.extend(db_tools.get_tool_definitions())

    # Add RAG tools
    tools.extend(rag_tools.get_tool_definitions())

    return tools


@app.call_tool()
async def call_tool(name: str, arguments: dict):
    """
    Route tool calls to appropriate tool module
    This is just routing logic - no business logic here
    """

    # Route to database tools
    if name in ["query_database", "get_documents_from_db", "get_user_info"]:
        return await db_tools.execute_tool(name, arguments)

    # Route to RAG tools
    if name in ["ask_rag", "list_documents", "upload_pdf"]:
        return await rag_tools.execute_tool(name, arguments)

    # Unknown tool
    return [{"type": "text", "text": f"Unknown tool: {name}"}]


async def main():
    """Run the MCP server"""
    try:
        # Initialize services
        db_service.connect()
        logger.info("MCP Server starting...")

        # Run server
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )

    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    finally:
        # Cleanup
        db_service.close()
        logger.info("MCP Server stopped")


if __name__ == "__main__":
    asyncio.run(main())