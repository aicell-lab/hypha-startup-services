"""Tests for the chunking utility module."""

import pytest

from hypha_startup_services.common.chunking import chunk_documents, chunk_text


class TestChunkText:
    """Test the chunk_text function."""

    def test_short_text_no_chunking(self) -> None:
        """Test that short text doesn't get chunked."""
        text = "This is a short text."
        chunks = chunk_text(text, chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_long_text_chunking(self) -> None:
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

    def test_empty_text(self) -> None:
        """Test handling of empty text."""
        chunks = chunk_text("", chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == ""

    def test_chunk_overlap(self) -> None:
        """Test that chunk overlap works as expected."""
        text = " ".join(["word"] * 100)
        chunks = chunk_text(text, chunk_size=30, chunk_overlap=5)

        if len(chunks) > 1:
            # There should be some content overlap between consecutive chunks
            assert sum(len(c) for c in chunks) > len(text)

    def test_custom_chunk_size(self) -> None:
        """Test different chunk sizes."""
        text = " ".join(["word"] * 100)

        small_chunks = chunk_text(text, chunk_size=20, chunk_overlap=5)
        large_chunks = chunk_text(text, chunk_size=200, chunk_overlap=5)

        # Smaller chunk size should produce more chunks
        if len(small_chunks) < len(large_chunks):
            pytest.fail("Smaller chunk size should produce more chunks")


class TestChunkDocuments:
    """Test the chunk_documents function."""

    def test_empty_documents(self) -> None:
        """Test handling of empty document list."""
        result = chunk_documents([], chunk_size=100, chunk_overlap=10)
        if result != []:
            pytest.fail("Expected empty list for empty documents input")

    def test_single_document(self) -> None:
        """Test chunking a single document."""
        documents = [{"text": "This is a test document.", "id": "doc1"}]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        if len(result) != 1:
            pytest.fail("Expected one chunk for short document")
        if result[0]["text"] != "This is a test document.":
            pytest.fail("Document text mismatch")
        if result[0]["id"] != "doc1":
            pytest.fail("Document ID mismatch")
        if result[0]["chunk_index"] != 0:
            pytest.fail("Chunk index mismatch")
        if result[0]["total_chunks"] != 1:
            pytest.fail("Total chunks mismatch")

    def test_multiple_documents(self) -> None:
        """Test chunking multiple documents."""
        documents = [
            {"text": "First document text.", "id": "doc1", "meta": "data1"},
            {"text": "Second document text.", "id": "doc2", "meta": "data2"},
        ]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        assert len(result) == len(
            documents,
        ), "Expected two chunks for two short documents"

        for chunk, doc in zip(result, documents, strict=False):
            assert chunk["text"] == doc["text"], f"Text mismatch for {doc['id']}"
            assert chunk["id"] == doc["id"], f"ID mismatch for {doc['id']}"
            assert chunk["meta"] == doc["meta"], f"Meta mismatch for {doc['id']}"
            assert chunk["chunk_index"] == 0, f"Chunk index mismatch for {doc['id']}"
            assert chunk["total_chunks"] == 1, f"Total chunks mismatch for {doc['id']}"

    def test_document_requiring_chunking(self) -> None:
        """Test a document that requires multiple chunks."""
        long_text = " ".join(["word"] * 200)
        documents = [{"text": long_text, "id": "long_doc"}]
        result = chunk_documents(documents, chunk_size=50, chunk_overlap=10)

        # Should produce multiple chunks for the long document
        assert len(result) > 1, "Should produce multiple chunks for the long document"

        for i, chunk in enumerate(result):
            assert chunk["id"] == "long_doc", "Document ID mismatch in chunks"
            assert "chunk_index" in chunk, "Missing chunk_index in chunk"
            assert "total_chunks" in chunk, "Missing total_chunks in chunk"
            assert chunk["chunk_index"] == i, "Chunk index mismatch"
            assert chunk["total_chunks"] == len(result), "Total chunks mismatch"

    def test_missing_text_field(self) -> None:
        """Test handling of documents without text field."""
        documents = [{"id": "doc1", "meta": "data"}]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        first_result = result[0]
        input_document = documents[0]
        if len(result) != 1:
            pytest.fail("Expected one chunk for document with missing text field")
        if first_result["text"] != "":
            pytest.fail("Expected empty text for document with missing text field")
        if first_result["id"] != input_document["id"]:
            pytest.fail("Document ID mismatch")
        if first_result["meta"] != input_document["meta"]:
            pytest.fail("Document meta mismatch")

    def test_preserve_metadata(self) -> None:
        """Test that document metadata is preserved in chunks."""
        documents: list[dict[str, str | float]] = [
            {
                "text": "Test document.",
                "id": "doc1",
                "author": "Test Author",
                "category": "Test Category",
                "score": 0.95,
            },
        ]
        result = chunk_documents(documents, chunk_size=100, chunk_overlap=10)

        if len(result) != 1:
            pytest.fail("Expected one chunk for short document")
        input_document = documents[0]
        chunk = result[0]
        if chunk["text"] != input_document["text"]:
            pytest.fail("Document text mismatch")
        if chunk["id"] != input_document["id"]:
            pytest.fail("Document ID mismatch")
        if chunk["author"] != input_document["author"]:
            pytest.fail("Document author mismatch")
        if chunk["category"] != input_document["category"]:
            pytest.fail("Document category mismatch")
        if chunk["score"] != input_document["score"]:
            pytest.fail("Document score mismatch")
        if chunk["chunk_index"] != 0:
            pytest.fail("Chunk index mismatch")
        if chunk["total_chunks"] != 1:
            pytest.fail("Total chunks mismatch")


class TestChunkingEdgeCases:
    """Test edge cases for chunking functions."""

    def test_none_text(self) -> None:
        """Test handling of None text."""
        chunks = chunk_text("", chunk_size=100, chunk_overlap=10)
        if len(chunks) != 1:
            pytest.fail("Expected one chunk for None text")
        if chunks[0] != "":
            pytest.fail("Expected empty string for None text")

    def test_zero_chunk_size(self) -> None:
        """Test handling of zero chunk size."""
        with pytest.raises(ValueError):  # noqa: PT011
            chunk_text("test", chunk_size=0, chunk_overlap=10)

    def test_negative_chunk_size(self) -> None:
        """Test handling of negative chunk size."""
        with pytest.raises(ValueError):  # noqa: PT011
            chunk_text("test", chunk_size=-1, chunk_overlap=10)

    def test_negative_overlap(self) -> None:
        """Test handling of negative overlap."""
        with pytest.raises(ValueError):  # noqa: PT011
            chunk_text("test", chunk_size=100, chunk_overlap=-1)

    def test_overlap_greater_than_chunk_size(self) -> None:
        """Test handling of overlap greater than chunk size."""
        with pytest.raises(ValueError):  # noqa: PT011
            chunk_text("test", chunk_size=50, chunk_overlap=100)
