# tools/rag_tools.py
"""
RAG MCP tools - MCP-specific wrappers around RAGService
Handles MCP schemas, formatting, and error translation
"""

import logging
from typing import List
from mcp.types import Tool, TextContent
from services.rag import RAGService
from utils.formatters import (
    format_rag_response,
    format_document_list,
    format_upload_result,
    format_error_message
)

logger = logging.getLogger(__name__)


class RAGTools:
    """MCP tools for RAG operations"""

    def __init__(self, rag_service: RAGService):
        self.rag = rag_service

    def get_tool_definitions(self) -> List[Tool]:
        """Return MCP tool schemas for RAG operations"""
        return [
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
            )
        ]

    async def execute_tool(self, name: str, arguments: dict) -> List[TextContent]:
        """
        Execute a RAG tool
        This is the MCP-specific execution logic that:
        1. Calls the service layer
        2. Formats results for MCP
        3. Handles MCP-specific error formatting
        """
        try:
            if name == "ask_rag":
                return await self._handle_ask_rag(arguments)

            elif name == "list_documents":
                return await self._handle_list_documents(arguments)

            elif name == "upload_pdf":
                return await self._handle_upload_pdf(arguments)

            else:
                raise ValueError(f"Unknown RAG tool: {name}")

        except Exception as e:
            error_message = format_error_message(e, name)
            logger.error(error_message)
            return [TextContent(type="text", text=error_message)]

    async def _handle_ask_rag(self, arguments: dict) -> List[TextContent]:
        """Handle document question answering"""
        question = arguments["question"]
        document_id = arguments.get("document_id")

        # Call service layer
        response = self.rag.ask_question(question, document_id)

        # Format for MCP using utility
        result_text = format_rag_response(response)

        return [TextContent(type="text", text=result_text)]

    async def _handle_list_documents(self, arguments: dict) -> List[TextContent]:
        """Handle listing all documents"""
        # Call service layer
        documents = self.rag.list_documents()

        # Format for MCP using utility
        result_text = format_document_list(documents)

        return [TextContent(type="text", text=result_text)]

    async def _handle_upload_pdf(self, arguments: dict) -> List[TextContent]:
        """Handle PDF upload"""
        file_path = arguments["file_path"]
        title = arguments.get("title")
        description = arguments.get("description")

        # Call service layer
        response = self.rag.upload_document(
            file_path=file_path,
            title=title,
            description=description
        )

        # Format for MCP using utility
        result_text = format_upload_result(response, file_path)

        return [TextContent(type="text", text=result_text)]