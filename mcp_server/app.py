#!/usr/bin/env python3
"""
MCP Server for RAG API + PostgreSQL + External APIs
Handles document Q&A, database queries, and external service calls
"""

import asyncio
import json
import os
import logging
from datetime import datetime
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# ============================================================================
# CONFIGURATION & LOGGING
# ============================================================================

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), 'mcp_server.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
RAG_API_URL = os.getenv("RAG_API_URL", "https://contextual-ai-cw79.onrender.com/")
RAG_API_KEY = os.getenv("RAG_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://yash1thsa:81LTYIIV6rFQ2Jj2FIn1PGeOTAomObKH@dpg-d4banuvdiees73af02r0-a.ohio-postgres.render.com/pdf_ingest")

# Validate configuration
if not RAG_API_KEY:
    logger.warning("RAG_API_KEY not set - RAG features may not work")


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

class DatabaseManager:
    """Manages PostgreSQL connections and queries"""

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

    def execute_query(self, query: str, params: tuple = None) -> list:
        """Execute a SELECT query safely"""
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

    def execute_command(self, command: str, params: tuple = None) -> int:
        """Execute INSERT/UPDATE/DELETE command"""
        try:
            if not self.connection or self.connection.closed:
                self.connect()

            with self.connection.cursor() as cursor:
                cursor.execute(command, params)
                self.connection.commit()
                affected_rows = cursor.rowcount
                logger.info(f"Command executed: {command[:100]}... | Affected rows: {affected_rows}")
                return affected_rows
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Command execution failed: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


# Initialize database manager
db = DatabaseManager(DATABASE_URL)


# ============================================================================
# RAG API CLIENT
# ============================================================================

class RAGClient:
    """Client for interacting with RAG API"""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })

    def ask_question(self, question: str, document_id: Optional[str] = None) -> dict:
        """Ask a question about documents"""
        try:
            payload = {
                'question': question,
                'document_id': document_id
            }

            response = self.session.post(
                f'{self.base_url}/query',
                json=payload,
                timeout=30
            )
            response.raise_for_status()

            result = response.json()
            logger.info(f"RAG question asked: {question[:50]}... | Status: {response.status_code}")
            return result

        except requests.Timeout:
            logger.error("RAG API request timed out")
            raise Exception("RAG API timeout - please try again")
        except requests.RequestException as e:
            logger.error(f"RAG API request failed: {e}")
            raise Exception(f"RAG API error: {str(e)}")

    def upload_pdf(self, file_path: str, metadata: dict = None) -> dict:
        """Upload a PDF to RAG system"""
        try:
            with open(file_path, 'rb') as f:
                files = {'file': f}
                data = {'metadata': json.dumps(metadata)} if metadata else {}

                response = self.session.post(
                    f'{self.base_url}/api/upload',
                    files=files,
                    data=data,
                    timeout=60
                )
                response.raise_for_status()

                result = response.json()
                logger.info(f"PDF uploaded: {file_path} | Document ID: {result.get('document_id')}")
                return result

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise Exception(f"File not found: {file_path}")
        except requests.RequestException as e:
            logger.error(f"PDF upload failed: {e}")
            raise Exception(f"Upload error: {str(e)}")

    def list_documents(self) -> list:
        """List all documents in RAG system"""
        try:
            response = self.session.get(
                f'{self.base_url}/api/documents',
                timeout=10
            )
            response.raise_for_status()

            documents = response.json()
            logger.info(f"Documents listed: {len(documents)} found")
            return documents

        except requests.RequestException as e:
            logger.error(f"List documents failed: {e}")
            raise Exception(f"Failed to list documents: {str(e)}")


# Initialize RAG client
rag = RAGClient(RAG_API_URL, RAG_API_KEY)


# ============================================================================
# EXTERNAL API CLIENT (Example)
# ============================================================================

class ExternalAPIClient:
    """Generic client for external APIs"""

    def __init__(self):
        self.session = requests.Session()

    def call_api(self, url: str, method: str = 'GET',
                 headers: dict = None, params: dict = None,
                 json_data: dict = None) -> dict:
        """Make a generic API call"""
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                timeout=15
            )
            response.raise_for_status()

            logger.info(f"External API called: {method} {url} | Status: {response.status_code}")
            return response.json()

        except requests.RequestException as e:
            logger.error(f"External API call failed: {e}")
            raise Exception(f"API call error: {str(e)}")


# Initialize external API client
external_api = ExternalAPIClient()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def format_database_results(results: list) -> str:
    """Format database query results as readable text"""
    if not results:
        return "No results found"

    # Format as a table-like structure
    output = []
    for i, row in enumerate(results, 1):
        output.append(f"\n--- Record {i} ---")
        for key, value in row.items():
            output.append(f"{key}: {value}")

    return '\n'.join(output)


def validate_sql_query(query: str) -> bool:
    """Basic SQL query validation (prevent dangerous operations)"""
    query_upper = query.upper().strip()

    # Block dangerous operations
    dangerous_keywords = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE']
    for keyword in dangerous_keywords:
        if keyword in query_upper:
            return False

    # Only allow SELECT queries
    if not query_upper.startswith('SELECT'):
        return False

    return True


def log_tool_usage(tool_name: str, arguments: dict, result: str, error: str = None):
    """Log tool usage for audit purposes"""
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'tool': tool_name,
        'arguments': arguments,
        'success': error is None,
        'error': error,
        'result_preview': result[:200] if result else None
    }
    logger.info(f"Tool used: {json.dumps(log_entry)}")


# ============================================================================
# MCP SERVER
# ============================================================================

app = Server("rag-db-mcp-server")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """Define all available tools"""
    return [
        # RAG API Tools
        Tool(
            name="ask_rag",
            description="Ask a question about documents in the RAG system. Returns AI-generated answers based on document content.",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The question to ask about the documents"
                    },
                    "document_id": {
                        "type": "string",
                        "description": "Optional: Specific document ID to query. Leave empty to search all documents."
                    }
                },
                "required": ["question"]
            }
        ),
        Tool(
            name="list_documents",
            description="List all documents available in the RAG system with their metadata",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="upload_pdf",
            description="Upload a PDF file to the RAG system for processing and Q&A",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Full path to the PDF file to upload"
                    },
                    "title": {
                        "type": "string",
                        "description": "Optional: Title for the document"
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional: Description of the document"
                    }
                },
                "required": ["file_path"]
            }
        ),

        # Database Tools
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
        ),

        # External API Tool
        Tool(
            name="call_external_api",
            description="Make a generic HTTP request to an external API",
            inputSchema={
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "The API endpoint URL"
                    },
                    "method": {
                        "type": "string",
                        "description": "HTTP method (GET, POST, etc.)",
                        "enum": ["GET", "POST", "PUT", "DELETE"]
                    },
                    "headers": {
                        "type": "object",
                        "description": "Optional: HTTP headers as key-value pairs"
                    },
                    "params": {
                        "type": "object",
                        "description": "Optional: Query parameters"
                    },
                    "json_data": {
                        "type": "object",
                        "description": "Optional: JSON body for POST/PUT requests"
                    }
                },
                "required": ["url"]
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool execution"""

    result_text = ""
    error_text = None

    try:
        # ===== RAG API TOOLS =====

        if name == "ask_rag":
            question = arguments["question"]
            document_id = arguments.get("document_id")

            # Validate input
            if len(question) > 1000:
                raise ValueError("Question too long (max 1000 characters)")

            # Call RAG API
            response = rag.ask_question(question, document_id)

            # Format response
            answer = response.get("answer", "No answer provided")
            sources = response.get("sources", [])
            confidence = response.get("confidence", "unknown")

            result_text = f"Answer: {answer}\n\n"
            result_text += f"Confidence: {confidence}\n\n"

            if sources:
                result_text += "Sources:\n"
                for i, source in enumerate(sources, 1):
                    result_text += f"{i}. {source}\n"

        elif name == "list_documents":
            documents = rag.list_documents()

            if not documents:
                result_text = "No documents found in the RAG system"
            else:
                result_text = f"Found {len(documents)} documents:\n\n"
                for i, doc in enumerate(documents, 1):
                    doc_id = doc.get("id", "unknown")
                    title = doc.get("title", "Untitled")
                    created = doc.get("created_at", "unknown")
                    result_text += f"{i}. {title}\n"
                    result_text += f"   ID: {doc_id}\n"
                    result_text += f"   Created: {created}\n\n"

        elif name == "upload_pdf":
            file_path = arguments["file_path"]

            # Build metadata
            metadata = {}
            if "title" in arguments:
                metadata["title"] = arguments["title"]
            if "description" in arguments:
                metadata["description"] = arguments["description"]

            # Upload to RAG
            response = rag.upload_pdf(file_path, metadata)

            doc_id = response.get("document_id", "unknown")
            status = response.get("status", "unknown")

            result_text = f"PDF uploaded successfully!\n"
            result_text += f"Document ID: {doc_id}\n"
            result_text += f"Status: {status}\n"

        # ===== DATABASE TOOLS =====

        elif name == "query_database":
            query = arguments["query"]

            # Validate query
            if not validate_sql_query(query):
                raise ValueError("Invalid query. Only SELECT queries are allowed. No DROP/DELETE/ALTER/etc.")

            # Execute query
            results = db.execute_query(query)

            # Format results
            result_text = f"Query executed successfully. Found {len(results)} rows.\n\n"
            result_text += format_database_results(results)

        elif name == "get_documents_from_db":
            limit = arguments.get("limit", 10)
            user_id = arguments.get("user_id")

            # Build query
            if user_id:
                query = "SELECT * FROM documents WHERE user_id = %s LIMIT %s"
                params = (user_id, limit)
            else:
                query = "SELECT * FROM documents LIMIT %s"
                params = (limit,)

            # Execute
            results = db.execute_query(query, params)

            result_text = f"Found {len(results)} documents:\n\n"
            result_text += format_database_results(results)

        elif name == "get_user_info":
            user_id = arguments["user_id"]

            # Query user
            query = "SELECT * FROM users WHERE id = %s"
            results = db.execute_query(query, (user_id,))

            if not results:
                result_text = f"User not found: {user_id}"
            else:
                result_text = "User information:\n\n"
                result_text += format_database_results(results)

        # ===== EXTERNAL API TOOL =====

        elif name == "call_external_api":
            url = arguments["url"]
            method = arguments.get("method", "GET")
            headers = arguments.get("headers")
            params = arguments.get("params")
            json_data = arguments.get("json_data")

            # Call API
            response = external_api.call_api(
                url=url,
                method=method,
                headers=headers,
                params=params,
                json_data=json_data
            )

            # Format response
            result_text = f"API call successful!\n\n"
            result_text += f"Response:\n{json.dumps(response, indent=2)}"

        else:
            raise ValueError(f"Unknown tool: {name}")

        # Log successful execution
        log_tool_usage(name, arguments, result_text)

        return [TextContent(
            type="text",
            text=result_text
        )]

    except Exception as e:
        # Log error
        error_text = str(e)
        log_tool_usage(name, arguments, "", error=error_text)

        # Return error message
        error_message = f"Error executing {name}: {error_text}"
        logger.error(error_message)

        return [TextContent(
            type="text",
            text=error_message
        )]


# ============================================================================
# SERVER STARTUP
# ============================================================================

async def main():
    """Run the MCP server"""
    try:
        # Initialize database connection
        db.connect()
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
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        # Cleanup
        db.close()
        logger.info("MCP Server stopped")


if __name__ == "__main__":
    asyncio.run(main())