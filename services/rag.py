# services/rag.py
"""
RAG service - Pure business logic, no MCP knowledge
Can be used by MCP, REST API, CLI, or any other interface
"""

import logging
import requests
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)


class RAGService:
    """Client for interacting with RAG API - Pure business operations"""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })

    def ask_question(self, question: str, document_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ask a question about documents
        Returns: Dict with 'query', 'answer', 'context' keys
        Raises: Exception if API call fails
        """
        # Validate input
        if not question or len(question.strip()) == 0:
            raise ValueError("Question cannot be empty")

        if len(question) > 1000:
            raise ValueError("Question too long (max 1000 characters)")

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

    def upload_document(self, file_path: str, title: Optional[str] = None,
                        description: Optional[str] = None) -> Dict[str, Any]:
        """
        Upload a PDF to RAG system
        Returns: Dict with 'document_id', 'status' keys
        Raises: Exception if upload fails or file not found
        """
        # Validate file exists
        import os
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Validate file type
        if not file_path.lower().endswith('.pdf'):
            raise ValueError("Only PDF files are supported")

        try:
            # Build metadata
            metadata = {}
            if title:
                metadata['title'] = title
            if description:
                metadata['description'] = description

            # Prepare upload
            with open(file_path, 'rb') as f:
                files = {'file': f}
                data = {'metadata': str(metadata)} if metadata else {}

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

        except requests.RequestException as e:
            logger.error(f"PDF upload failed: {e}")
            raise Exception(f"Upload error: {str(e)}")

    def list_documents(self) -> List[Dict[str, Any]]:
        """
        List all documents in RAG system
        Returns: List of document dicts with 'id', 'title', 'created_at' keys
        Raises: Exception if API call fails
        """
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

    def get_document_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific document by ID
        Returns: Document dict or None if not found
        """
        try:
            response = self.session.get(
                f'{self.base_url}/api/documents/{document_id}',
                timeout=10
            )

            if response.status_code == 404:
                return None

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            logger.error(f"Get document failed: {e}")
            raise Exception(f"Failed to get document: {str(e)}")