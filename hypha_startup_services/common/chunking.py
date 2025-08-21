"""Text chunking utilities."""

from typing import Any

import tiktoken


def chunk_text(
    text: str,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    encoding_name: str = "cl100k_base",
) -> list[str]:
    """Chunk text into smaller pieces using tiktoken encoding.

    Args:
        text: The text to chunk (can be None)
        chunk_size: Maximum number of tokens per chunk
        chunk_overlap: Number of tokens to overlap between chunks
        encoding_name: Tiktoken encoding name to use

    Returns:
        List of text chunks

    Raises:
        ValueError: If chunk_size <= 0, chunk_overlap < 0, or chunk_overlap >= chunk_size

    """
    # Validate parameters
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    if chunk_overlap < 0:
        raise ValueError("chunk_overlap must be non-negative")
    if chunk_overlap >= chunk_size:
        raise ValueError("chunk_overlap must be less than chunk_size")

    # Handle None or empty text
    if not text:
        return [""]

    encoding = tiktoken.get_encoding(encoding_name)
    tokens = encoding.encode(text)

    if len(tokens) <= chunk_size:
        return [text]

    chunks = []
    start = 0

    while start < len(tokens):
        end = min(start + chunk_size, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk = encoding.decode(chunk_tokens)
        chunks.append(chunk)

        # Move start position with overlap
        start = max(start + chunk_size - chunk_overlap, start + 1)

        if start >= len(tokens):
            break

    return chunks


def chunk_documents(
    documents: list[dict[str, Any]],
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    encoding_name: str = "cl100k_base",
) -> list[dict[str, Any]]:
    """Chunk multiple documents and return with metadata.

    Args:
        documents: List of document dictionaries (must have 'text' key)
        chunk_size: Maximum number of tokens per chunk
        chunk_overlap: Number of tokens to overlap between chunks
        encoding_name: Tiktoken encoding name to use

    Returns:
        List of document chunks with preserved metadata and chunk information

    """
    chunked_docs = []

    for doc in documents:
        # Get text from document, default to empty string if missing
        doc_text = doc.get("text", "")

        # Chunk the text
        chunks = chunk_text(doc_text, chunk_size, chunk_overlap, encoding_name)

        # Create new document for each chunk, preserving all original metadata
        for chunk_idx, chunk in enumerate(chunks):
            # Copy all original document fields
            chunked_doc = doc.copy()

            # Update text and add chunk metadata
            chunked_doc["text"] = chunk
            chunked_doc["chunk_index"] = chunk_idx
            chunked_doc["total_chunks"] = len(chunks)

            chunked_docs.append(chunked_doc)

    return chunked_docs
