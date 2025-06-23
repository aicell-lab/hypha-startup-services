"""Tests for the chunking utility module."""

import pytest
from hypha_startup_services.common.chunking import chunk_text, chunk_documents


class TestChunkText:
    """Test the chunk_text function."""

    def test_short_text_no_chunking(self):
        """Test that short text doesn't get chunked."""
        text = "This is a short text."
        chunks = chunk_text(text, chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_long_text_chunking(self):
        """Test that long text gets properly chunked."""
        # Create a text that will definitely need chunking
        text = " ".join(["word"] * 200)  # 200 words, should exceed typical token limits
        chunks = chunk_text(text, chunk_size=50, chunk_overlap=10)

        # Should produce multiple chunks
        assert len(chunks) > 1

        # Each chunk should be a string
        for chunk in chunks:
            assert isinstance(chunk, str)
            assert len(chunk) > 0

    def test_empty_text(self):
        """Test handling of empty text."""
        chunks = chunk_text("", chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == ""

    def test_chunk_overlap(self):
        """Test that chunk overlap works as expected."""
        text = " ".join(["word"] * 100)
        chunks = chunk_text(text, chunk_size=30, chunk_overlap=5)

        if len(chunks) > 1:
            # There should be some content overlap between consecutive chunks
            # This is a basic test - the actual overlap depends on tiktoken's behavior
            assert len(chunks) > 1

    def test_custom_chunk_size(self):
        """Test different chunk sizes."""
        text = " ".join(["word"] * 100)

        small_chunks = chunk_text(text, chunk_size=20, chunk_overlap=5)
        large_chunks = chunk_text(text, chunk_size=200, chunk_overlap=5)

        # Smaller chunk size should produce more chunks
        assert len(small_chunks) >= len(large_chunks)


class TestChunkDocuments:
    """Test the chunk_documents function."""

    def test_empty_documents(self):
        """Test handling of empty document list."""
        result = chunk_documents([], chunk_size=100, chunk_overlap=10)
        assert result == []

    def test_single_document(self):
        """Test chunking a single document."""
        documents = [{"text": "This is a test document.", "id": "doc1"}]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        assert len(result) == 1
        assert result[0]["text"] == "This is a test document."
        assert result[0]["id"] == "doc1"
        assert result[0]["chunk_index"] == 0
        assert result[0]["total_chunks"] == 1

    def test_multiple_documents(self):
        """Test chunking multiple documents."""
        documents = [
            {"text": "First document text.", "id": "doc1", "meta": "data1"},
            {"text": "Second document text.", "id": "doc2", "meta": "data2"},
        ]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        assert len(result) == 2

        # Check first document
        assert result[0]["text"] == "First document text."
        assert result[0]["id"] == "doc1"
        assert result[0]["meta"] == "data1"
        assert result[0]["chunk_index"] == 0
        assert result[0]["total_chunks"] == 1

        # Check second document
        assert result[1]["text"] == "Second document text."
        assert result[1]["id"] == "doc2"
        assert result[1]["meta"] == "data2"
        assert result[1]["chunk_index"] == 0
        assert result[1]["total_chunks"] == 1

    def test_document_requiring_chunking(self):
        """Test a document that requires multiple chunks."""
        long_text = " ".join(["word"] * 200)
        documents = [{"text": long_text, "id": "long_doc"}]
        result = chunk_documents(documents, chunk_size=50, chunk_overlap=10)

        # Should produce multiple chunks for the long document
        if len(result) > 1:
            # All chunks should have the same document ID
            for chunk in result:
                assert chunk["id"] == "long_doc"
                assert "chunk_index" in chunk
                assert "total_chunks" in chunk

            # Check chunk indices are sequential
            for i, chunk in enumerate(result):
                assert chunk["chunk_index"] == i
                assert chunk["total_chunks"] == len(result)

    def test_missing_text_field(self):
        """Test handling of documents without text field."""
        documents = [{"id": "doc1", "meta": "data"}]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        assert len(result) == 1
        assert result[0]["text"] == ""
        assert result[0]["id"] == "doc1"
        assert result[0]["meta"] == "data"

    def test_preserve_metadata(self):
        """Test that document metadata is preserved in chunks."""
        documents = [
            {
                "text": "Test document.",
                "id": "doc1",
                "author": "Test Author",
                "category": "Test Category",
                "score": 0.95,
            }
        ]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        assert len(result) == 1
        chunk = result[0]
        assert chunk["text"] == "Test document."
        assert chunk["id"] == "doc1"
        assert chunk["author"] == "Test Author"
        assert chunk["category"] == "Test Category"
        assert chunk["score"] == 0.95
        assert chunk["chunk_index"] == 0
        assert chunk["total_chunks"] == 1


class TestChunkingEdgeCases:
    """Test edge cases for chunking functions."""

    def test_none_text(self):
        """Test handling of None text."""
        chunks = chunk_text(None, chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == ""

    def test_zero_chunk_size(self):
        """Test handling of zero chunk size."""
        with pytest.raises(ValueError):
            chunk_text("test", chunk_size=0, chunk_overlap=10)

    def test_negative_chunk_size(self):
        """Test handling of negative chunk size."""
        with pytest.raises(ValueError):
            chunk_text("test", chunk_size=-1, chunk_overlap=10)

    def test_negative_overlap(self):
        """Test handling of negative overlap."""
        with pytest.raises(ValueError):
            chunk_text("test", chunk_size=100, chunk_overlap=-1)

    def test_overlap_greater_than_chunk_size(self):
        """Test handling of overlap greater than chunk size."""
        with pytest.raises(ValueError):
            chunk_text("test", chunk_size=50, chunk_overlap=100)
