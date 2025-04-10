"""
HyphaArtifact module implements an fsspec-compatible interface for Hypha artifacts.

This module provides a file-system like interface to interact with remote Hypha artifacts
using the fsspec specification, allowing for operations like reading, writing, listing,
and manipulating files stored in Hypha artifacts.
"""

import io
import locale
import os
import sys
import asyncio
import logging
from typing import Literal, Self, overload
import jwt
from dotenv import load_dotenv
import requests
from hypha_rpc import connect_to_server

# Set up logger
logger = logging.getLogger("hypha_artifact")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

type OnError = Literal["raise", "ignore"]
type JsonType = str | int | float | bool | None | dict[str, "JsonType"] | list[
    "JsonType"
]
type FileMode = Literal["r", "rb", "w", "wb", "a", "ab"]
type UploadMethod = Literal["POST", "PUT"]


def ws_from_token(token: str) -> str:
    """Extracts the workspace ID from the token."""
    decoded_token = jwt.decode(token, options={"verify_signature": False})
    scope = decoded_token.get("scope", "")
    workspace_parts = [part for part in scope.split() if part.startswith("ws:")]
    return workspace_parts[0].split(":", 1)[1].split("#", 1)[0]


class WithHTTPFile(io.IOBase):
    """A file-like object that supports both sync and async context manager protocols.

    This implements a file interface for Hypha artifacts, handling HTTP operations
    via the requests library instead of relying on Pyodide.
    """

    def __init__(
        self: Self,
        url: str,
        mode: FileMode = "r",
        encoding: str | None = None,
        newline: str | None = None,
        name: str | None = None,
        upload_method: UploadMethod = "POST",
    ):
        self._url = url
        self._pos = 0
        self._mode = mode
        self._encoding = encoding or locale.getpreferredencoding()
        self._newline = newline or os.linesep
        self.name = name
        self._upload_method = upload_method
        self._closed = False
        self._buffer = io.BytesIO()

        if "r" in mode:
            # For read mode, download the content immediately
            self._download_content()
            self._size = len(self._buffer.getvalue())
        else:
            # For write modes, initialize an empty buffer
            self._size = 0

    def _download_content(self):
        """Download content from URL into buffer"""
        try:
            headers = {}
            response = requests.get(self._url, headers=headers, timeout=60)
            response.raise_for_status()
            self._buffer = io.BytesIO(response.content)
        except requests.exceptions.RequestException as e:
            # More detailed error information for debugging
            status_code = (
                e.response.status_code if hasattr(e, "response") else "unknown"
            )
            message = str(e)
            raise IOError(
                f"Error downloading content (status {status_code}): {message}"
            ) from e
        except Exception as e:
            raise IOError(f"Unexpected error downloading content: {str(e)}") from e

    def _upload_to_s3(self, content):
        """Handle upload specifically for S3 pre-signed URLs"""
        try:
            logger.info("Uploading %d bytes to S3 URL", len(content))

            # S3 pre-signed URLs require specific headers
            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(content)),
            }

            # Always use PUT for S3 pre-signed URLs
            response = requests.put(
                self._url, data=content, headers=headers, timeout=60
            )

            if response.status_code >= 400:
                logger.error(
                    "S3 upload failed with status %s: %s",
                    response.status_code,
                    response.text,
                )

            response.raise_for_status()
            logger.info("S3 upload completed successfully")
            return response
        except Exception as e:
            logger.error("S3 upload error: %s", str(e))
            raise IOError(f"Error uploading to S3: {str(e)}") from e

    def _upload_content(self):
        """Upload buffer content to URL"""
        try:
            content = self._buffer.getvalue()

            # Check if this is an S3 URL
            if "s3.amazonaws.com" in self._url or ".s3." in self._url:
                return self._upload_to_s3(content)

            headers = {"Content-Type": "application/octet-stream"}
            logger.debug(
                "Uploading content to %s using %s", self._url, self._upload_method
            )

            if self._upload_method == "PUT":
                response = requests.put(
                    self._url, data=content, headers=headers, timeout=60
                )
            else:  # Default to POST
                files = {"file": ("blob", content, "application/octet-stream")}
                response = requests.post(self._url, files=files, timeout=60)

            if response.status_code >= 400:
                logger.error(
                    f"Upload failed with status {response.status_code}: {response.text}"
                )

            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            status_code = (
                e.response.status_code if hasattr(e, "response") else "unknown"
            )
            error_msg = e.response.text if hasattr(e, "response") else str(e)
            raise IOError(
                f"HTTP error uploading content (status {status_code}): {error_msg}"
            ) from e
        except Exception as e:
            raise IOError(f"Error uploading content: {str(e)}") from e

    def tell(self):
        """Return current position in the file"""
        return self._pos

    def seek(self, offset, whence=0):
        """Change stream position"""
        if whence == 0:  # os.SEEK_SET
            self._pos = offset
        elif whence == 1:  # os.SEEK_CUR
            self._pos += offset
        elif whence == 2:  # os.SEEK_END
            self._pos = self._size + offset

        # Make sure buffer's position is synced
        self._buffer.seek(self._pos)
        return self._pos

    def read(self, size=-1):
        """Read up to size bytes from the file"""
        if "r" not in self._mode:
            raise IOError("File not open for reading")

        self._buffer.seek(self._pos)
        if size < 0:
            data = self._buffer.read()
        else:
            data = self._buffer.read(size)

        self._pos = self._buffer.tell()

        if "b" not in self._mode:
            return data.decode(self._encoding)
        return data

    def write(self, data):
        """Write data to the file"""
        if "w" not in self._mode and "a" not in self._mode:
            raise IOError("File not open for writing")

        # Convert string to bytes if necessary
        if isinstance(data, str) and "b" in self._mode:
            data = data.encode(self._encoding)
        elif isinstance(data, bytes) and "b" not in self._mode:
            data = data.decode(self._encoding)
            data = data.encode(self._encoding)

        # Ensure we're at the right position
        self._buffer.seek(self._pos)

        # Write the data
        if isinstance(data, str):
            bytes_written = self._buffer.write(data.encode(self._encoding))
        else:
            bytes_written = self._buffer.write(data)

        self._pos = self._buffer.tell()
        if self._pos > self._size:
            self._size = self._pos

        return bytes_written

    def readable(self):
        """Return whether the file is readable"""
        return "r" in self._mode

    def writable(self):
        """Return whether the file is writable"""
        return "w" in self._mode or "a" in self._mode

    def seekable(self):
        """Return whether the file is seekable"""
        return True

    def close(self):
        """Close the file and upload content if in write mode"""
        if self._closed:
            return

        try:
            if "w" in self._mode or "a" in self._mode:
                self._upload_content()
        finally:
            self._closed = True
            self._buffer.close()

    def __enter__(self: Self) -> Self:
        """Enter context manager"""
        return self

    def __exit__(self: Self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager"""
        self.close()

    def __aenter__(self: Self) -> Self:
        """Enter async context manager"""
        return self

    def __aexit__(self: Self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager"""
        self.close()


class HyphaArtifact:
    artifact_id: str
    artifact_url: str
    token: str
    workspace_id: str

    def __init__(self: Self, artifact_id: str):
        """Initialize a HyphaArtifact instance.

        Parameters
        ----------
        artifact_id: str
            The identifier of the Hypha artifact to interact with
        """
        load_dotenv()
        self.artifact_id = artifact_id
        self.token = os.getenv("PERSONAL_TOKEN")
        self.workspace_id = ws_from_token(self.token)
        self.artifact_url = "https://hypha.aicell.io/public/services/artifact-manager"

    def _extend_params(
        self: Self,
        params: dict[str, JsonType],
    ) -> dict[str, JsonType]:
        params["artifact_id"] = self.artifact_id
        params["workspace"] = self.workspace_id

        return params

    def _remote_post(self: Self, method_name: str, params: dict[str, JsonType]) -> str:
        """Make a POST request to the artifact service with extended parameters.

        Returns:
            For put_file requests, returns the pre-signed URL as a string.
            For other requests, returns the response content.
        """
        extended_params = self._extend_params(params)

        response = requests.post(
            f"{self.artifact_url}/{method_name}",
            params=extended_params,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=60,  # Increased timeout
        )

        response.raise_for_status()

        # Handle put_file specially to return the URL string
        if method_name == "put_file":
            try:
                # Try to decode as JSON first
                url_data = response.json()
                if isinstance(url_data, dict) and "url" in url_data:
                    return url_data["url"]
                return url_data
            except Exception as e:
                return response.content.decode("utf-8").strip('"')

        return response.content

    def _remote_get(self: Self, method_name: str, params: dict[str, JsonType]) -> None:
        extended_params = self._extend_params(params)
        response = requests.get(
            f"{self.artifact_url}/{method_name}",
            params=extended_params,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=20,
        )

        response.raise_for_status()
        return response.content

    def _remote_edit(
        self: Self,
        manifest: dict | None = None,
        artifact_type: str | None = None,
        permissions: dict | None = None,
        config: dict | None = None,
        secrets: dict | None = None,
        version: str | None = None,
        comment: str | None = None,
        copy_files: bool | None = None,
    ) -> None:
        """Edits the artifact's metadata, including the manifest, type, and permissions.

        Args:
            self (Self): The instance of the HyphaArtifact class.
            manifest (dict | None): Optional. The updated manifest.
                Ensure the manifest follows the required schema
                if applicable (e.g., for collections).
            artifact_type (str | None): Optional. The type of the artifact.
                Supported values are collection, generic and any other custom type.
                By default, it's set to generic which contains fields tailored for
                displaying the artifact as cards on a webpage.
            permissions (dict | None): Optional. A dictionary containing user permissions.
                For example {"*": "r+"} gives read and create access to everyone,
                {"@": "rw+"} allows all authenticated users to read/write/create,
                and {"user_id_1": "r+"} grants read and create permissions to a specific user.
                You can also set permissions for specific operations,
                such as {"user_id_1": ["read", "create"]}.
                See detailed explanation about permissions below.
            secrets (dict | None): Optional. A dictionary containing secrets to be stored
                with the artifact. Secrets are encrypted and can only be accessed
                by the artifact owner or users with appropriate permissions.
                See the create function for a list of supported secrets.
            config (dict | None): Optional. Optional. A dictionary containing additional
                configuration options for the artifact.
            version (str | None): Optional. Optional. The version of the artifact to edit.
                By default, it set to None, the version will stay the same.
                If you want to create a staged version, you can set it to "stage".
                You can set it to any version in text, e.g. 0.1.0 or v1.
                If you set it to "new", it will generate a version similar to v0, v1, etc.
            comment (str | None): Optional. A comment to describe the changes made to the artifact.
            copy_files (bool | None): Optional. A boolean flag indicating whether to copy files
                from the previous version when creating a new staged version.
                Default is None. Set to True to copy files from the previous version.
        """

        params = {
            "manifest": manifest,
            "artifact_type": artifact_type,
            "permissions": permissions,
            "config": config,
            "secrets": secrets,
            "version": version,
            "comment": comment,
            "copy_files": copy_files,
        }
        self._remote_post("edit", params)

    def _remote_commit(
        self: Self, version: str | None = None, comment: str | None = None
    ) -> None:
        """Finalizes and commits an artifact's staged changes.
            Validates uploaded files and commits the staged manifest.
            This process also updates view and download statistics.

        Args:
            self (Self): The instance of the HyphaArtifact class.
            version (str | None): Optional. The version of the artifact to edit.
                By default, it set to None, the version will stay the same.
                If you want to create a staged version, you can set it to "stage".
                You can set it to any version in text, e.g. 0.1.0 or v1. If you set it to new,
                it will generate a version similar to v0, v1, etc.
            comment (str | None): Optional. A comment to describe the changes made to the artifact.
        """
        params = {
            "version": version,
            "comment": comment,
        }
        self._remote_post("commit", params)

    def _remote_put_file(
        self: Self,
        file_path: str,
        download_weight: float | None = None,
    ) -> str:
        """Generates a pre-signed URL to upload a file to the artifact in S3. The URL can be used
        with an HTTP PUT request to upload the file. The file is staged until the
        artifact is committed.

        Args:
            self (Self): The instance of the HyphaArtifact class.
            file_path (str): The relative path of the file to upload within the
                artifact (e.g., "data.csv").
            download_weight (float | None): A float value representing the file's impact
                on download count (0-1). Defaults to None.

        Returns:
            str: A pre-signed URL for uploading the file.
        """
        params = {
            "file_path": file_path,
        }
        # Only include download_weight if it's not None
        if download_weight is not None:
            params["download_weight"] = download_weight

        response = self._remote_post("put_file", params)
        return response.text

    def _remote_remove_file(
        self: Self,
        file_path: str,
    ) -> None:
        """Removes a file from the artifact and updates the staged manifest. The file is
        also removed from the S3 storage.

        Args:
            self (Self): The instance of the HyphaArtifact class.
            file_path (str): The relative path of the file to be removed (e.g., "data.csv").
        """
        params = {
            "file_path": file_path,
        }
        self._remote_post("remove_file", params)

    def _remote_get_file(self: Self, file_path: str, silent: bool = False) -> str:
        """Generates a pre-signed URL to download a file from the artifact stored in S3.

        Args:
            self (Self): The instance of the HyphaArtifact class.
            file_path (str): The relative path of the file to be downloaded (e.g., "data.csv").
            silent (bool, optional): A boolean to suppress the download count increment.
                Default is False.

        Returns:
            str: A pre-signed URL for downloading the file.
        """
        params = {
            "file_path": file_path,
            "silent": silent,
        }
        return self._remote_get("get_file", params)

    def _remote_list_files(self: Self, dir_path: str | None = None) -> list[JsonType]:
        """Lists all files in the artifact.

        Args:
            self (Self): The instance of the HyphaArtifact class.
            dir_path (str | None, optional): Optional. The directory path within the artifact to
                list files from. Default is None.

        Returns:
            list[JsonType]: A list of files in the specified directory.
        """
        params = {
            "dir_path": dir_path,
        }
        return self._remote_get("list_files", params)

    @overload
    def cat(
        self: Self,
        path: list[str],
        recursive: bool = False,
        on_error: OnError | None = None,
    ) -> dict[str, str]: ...

    @overload
    def cat(
        self: Self, path: str, recursive: bool = False, on_error: OnError | None = None
    ) -> str: ...

    def cat(
        self: Self,
        path: str | list[str],
        recursive: bool = False,
        on_error: OnError = "raise",
    ) -> dict[str, str] | str:
        """Get file(s) content as string(s)

        Parameters
        ----------
        path: str or list of str
            File path(s) to get content from
        recursive: bool
            If True and path is a directory, get all files content
        on_error: "raise" or "ignore"
            What to do if a file is not found

        Returns
        -------
        str or dict
            File contents as string if path is a string, or dict of {path: content} if path is a list
        """
        # Handle the case where path is a list of paths
        if isinstance(path, list):
            result = {}
            for p in path:
                try:
                    result[p] = self.cat(p, recursive=recursive, on_error=on_error)
                except Exception:
                    if on_error == "raise":
                        raise
            return result

        # Handle recursive case
        if recursive and self.isdir(path):
            files = self.find(path, maxdepth=None)
            return {p: self.cat(p, on_error=on_error) for p in files}

        # Handle single file case
        try:
            with self.open(path, mode="r") as file:
                return file.read()
        except Exception:
            if on_error == "raise":
                raise
            return None

    def open(self: Self, urlpath: str, mode: FileMode = "rb", **kwargs) -> WithHTTPFile:
        """Open a file for reading or writing

        Parameters
        ----------
        urlpath: str
            Path to the file within the artifact
        mode: FileMode
            File mode, one of 'r', 'rb', 'w', 'wb', 'a', 'ab'

        Returns
        -------
        WithHTTPFile
            A file-like object
        """
        if "r" in mode:
            # Reading mode
            download_url = self._remote_get_file(urlpath)
            return WithHTTPFile(
                url=download_url, mode=mode, name=urlpath, upload_method="PUT", **kwargs
            )
        elif "w" in mode or "a" in mode:
            # Writing or appending mode
            upload_url = self._remote_put_file(urlpath)
            return WithHTTPFile(
                url=upload_url, mode=mode, name=urlpath, upload_method="PUT", **kwargs
            )
        else:
            raise ValueError(f"Unsupported file mode: {mode}")

    def copy(
        self: Self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        on_error: OnError = "raise",
        **kwargs,
    ) -> None:
        """Copy file(s) from path1 to path2 within the artifact

        Parameters
        ----------
        path1: str
            Source path
        path2: str
            Destination path
        recursive: bool
            If True and path1 is a directory, copy all its contents recursively
        maxdepth: int or None
            Maximum recursion depth when recursive=True
        on_error: "raise" or "ignore"
            What to do if a file is not found
        """
        try:
            # Stage the artifact for edits
            self._remote_edit(version="stage")

            # Handle recursive case
            if recursive and self.isdir(path1):
                files = self.find(path1, maxdepth=maxdepth)
                for file_path in files:
                    # Calculate the destination path
                    rel_path = file_path[len(path1) :].lstrip("/")
                    dest_path = f"{path2}/{rel_path}" if path2 else rel_path
                    try:
                        self._copy_single_file(file_path, dest_path)
                    except Exception:
                        if on_error == "raise":
                            raise
            else:
                # Copy a single file
                self._copy_single_file(path1, path2)
        finally:
            # Commit changes
            self._remote_commit(version="new")

    def _copy_single_file(self, src: str, dst: str) -> None:
        """Helper method to copy a single file"""
        content = self.cat(src)
        with self.open(dst, mode="w") as f:
            f.write(content)

    def cp(
        self: Self, path1: str, path2: str, on_error: OnError | None = None, **kwargs
    ):
        """Alias for copy method

        Parameters
        ----------
        path1: str
            Source path
        path2: str
            Destination path
        on_error: "raise" or "ignore", optional
            What to do if a file is not found
        **kwargs:
            Additional arguments passed to copy method

        Returns
        -------
        None
        """
        return self.copy(path1, path2, on_error=on_error, **kwargs)

    def rm(
        self: Self, path: str, recursive: bool = False, maxdepth: int | None = None
    ) -> None:
        """Remove a file from the artifact

        Parameters
        ----------
        path: str
            Path to the file to remove
        recursive: bool, optional
            Not used, included for fsspec compatibility
        maxdepth: int or None, optional
            Not used, included for fsspec compatibility

        Returns
        -------
        None
        """
        # recursive and maxdepth are ignored as Hypha artifacts handle files individually
        return self._remote_remove_file(path)

    def created(self: Self, path: str):
        """Get the creation time of a file

        In the Hypha artifact system, we might not have direct access to creation time,
        but we can retrieve this information from file metadata if available.

        Parameters
        ----------
        path: str
            Path to the file

        Returns
        -------
        datetime or None
            Creation time of the file, if available
        """
        info = self.info(path)
        # Return creation time if available in the metadata, otherwise None
        return info.get("created") if info else None

    def delete(
        self: Self, path: str, recursive: bool = False, maxdepth: int | None = None
    ):
        return self.rm(path, recursive, maxdepth)

    def exists(self: Self, path: str, **kwargs) -> bool:
        """Check if a file or directory exists

        Parameters
        ----------
        path: str
            Path to check

        Returns
        -------
        bool
            True if the path exists, False otherwise
        """
        try:
            # Try to get file info - if it doesn't exist, this will fail
            self.info(path)
            return True
        except Exception:
            return False

    def ls(self: Self, path: str, detail: bool = True, **kwargs) -> list[str | dict]:
        """List files in a directory

        Parameters
        ----------
        path: str
            Path to list
        detail: bool
            If True, return a list of dictionaries with file details
            If False, return a list of file paths

        Returns
        -------
        list
            List of files in the directory
        """
        # First, get the list of files
        result = self._remote_list_files(path)

        if not detail:
            # Return only the file paths
            return [item["name"] for item in result]

        # Return the full file details
        return result

    def info(self: Self, path: str, **kwargs) -> dict[str, str | int]:
        """Get information about a file or directory

        Parameters
        ----------
        path: str
            Path to get information about

        Returns
        -------
        dict
            Dictionary with file information
        """
        # First check if it's in the listing of its parent directory
        parent_path = self._get_parent_path(path)

        try:
            listing = self.ls(parent_path)
            for item in listing:
                if item["name"] == path:
                    return item
        except Exception:
            pass

        # If we couldn't find it that way, check if it exists directly
        try:
            # Try to get a download URL - this will fail if the file doesn't exist
            self._remote_get_file(path, silent=True)

            # If we reached here, the file exists but we don't have details
            # Return minimal info
            return {"name": path, "size": None, "type": "file"}  # Size unknown
        except Exception:
            # Try as a directory
            try:
                listing = self._remote_list_files(path)
                # If we get here, it's a directory
                return {"name": path, "size": 0, "type": "directory"}
            except Exception:
                raise FileNotFoundError(f"Path not found: {path}")

    def _get_parent_path(self, path: str) -> str:
        """Get the parent directory of a path"""
        parts = path.rstrip("/").split("/")
        if len(parts) == 1:
            return ""  # Root directory
        return "/".join(parts[:-1])

    def isdir(self: Self, path: str) -> bool:
        """Check if a path is a directory

        Parameters
        ----------
        path: str
            Path to check

        Returns
        -------
        bool
            True if the path is a directory, False otherwise
        """
        try:
            info = self.info(path)
            return info["type"] == "directory"
        except Exception:
            return False

    def isfile(self: Self, path: str) -> bool:
        """Check if a path is a file

        Parameters
        ----------
        path: str
            Path to check

        Returns
        -------
        bool
            True if the path is a file, False otherwise
        """
        try:
            info = self.info(path)
            return info["type"] == "file"
        except Exception:
            return False

    def listdir(self: Self, path: str, **kwargs) -> list[str]:
        return self.ls(path, **kwargs)

    def find(
        self: Self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        detail: bool = False,
        **kwargs,
    ) -> list[str] | dict[str, dict]:
        """Find all files (and optional directories) under a path

        Parameters
        ----------
        path: str
            Base path to search from
        maxdepth: int or None
            Maximum recursion depth when searching
        withdirs: bool
            Whether to include directories in the results
        detail: bool
            If True, return a dict of {path: info_dict}
            If False, return a list of paths

        Returns
        -------
        list or dict
            List of paths or dict of {path: info_dict}
        """

        # Helper function to walk the directory tree recursively
        def _walk_dir(current_path, current_depth):
            results = {}

            # List current directory
            try:
                items = self.ls(current_path)
            except Exception:
                return {}

            # Add items to results
            for item in items:
                item_type = item.get("type")
                item_name = item.get("name")

                if item_type == "file" or (withdirs and item_type == "directory"):
                    results[item_name] = item

                # Recurse into subdirectories if depth allows
                if item_type == "directory" and (
                    maxdepth is None or current_depth < maxdepth
                ):
                    subdirectory_results = _walk_dir(item_name, current_depth + 1)
                    results.update(subdirectory_results)

            return results

        # Start the recursive walk
        all_files = _walk_dir(path, 1)

        if detail:
            return all_files
        else:
            return sorted(all_files.keys())

    def mkdir(self: Self, path: str, create_parents: bool = True, **kwargs) -> None:
        """Create a directory

        In the Hypha artifact system, directories don't need to be explicitly created,
        they are implicitly created when files are added under a path.
        However, we'll implement this as a no-op to maintain compatibility.

        Parameters
        ----------
        path: str
            Path to create
        create_parents: bool
            If True, create parent directories if they don't exist
        """
        # Directories in Hypha artifacts are implicit
        # This is a no-op for compatibility with fsspec

    def makedirs(self: Self, path: str, exist_ok: bool = True, **kwargs) -> None:
        """Create a directory and any parent directories

        In the Hypha artifact system, directories don't need to be explicitly created,
        they are implicitly created when files are added under a path.
        However, we'll implement this as a no-op to maintain compatibility.

        Parameters
        ----------
        path: str
            Path to create
        exist_ok: bool
            If False and the directory exists, raise an error
        """
        # If the directory already exists and exist_ok is False, raise an error
        if not exist_ok and self.exists(path) and self.isdir(path):
            raise FileExistsError(f"Directory already exists: {path}")

    def rm_file(self: Self, path: str) -> None:
        """Remove a file

        Parameters
        ----------
        path: str
            Path to remove
        """
        try:
            # Stage the artifact for edits
            self._remote_edit(version="stage")
            self._remote_remove_file(path)
        finally:
            # Commit changes
            self._remote_commit(version="new")

    def rmdir(self: Self, path: str) -> None:
        """Remove an empty directory

        In the Hypha artifact system, directories are implicit, so this would
        only make sense if the directory is empty. Since empty directories
        don't really exist explicitly, this is essentially a validation check
        that no files exist under this path.

        Parameters
        ----------
        path: str
            Path to remove
        """
        # Check if the directory exists
        if not self.isdir(path):
            raise FileNotFoundError(f"Directory not found: {path}")

        # Check if the directory is empty
        files = self.ls(path)
        if files:
            raise OSError(f"Directory not empty: {path}")

        # If we get here, the directory is empty (or doesn't exist),
        # so there's nothing to do

    def head(self: Self, path: str, size: int = 1024) -> bytes:
        """Get the first bytes of a file

        Parameters
        ----------
        path: str
            Path to the file
        size: int
            Number of bytes to read

        Returns
        -------
        bytes
            First bytes of the file
        """
        with self.open(path, mode="rb") as f:
            return f.read(size)

    def size(self: Self, path: str) -> int:
        """Get the size of a file in bytes

        Parameters
        ----------
        path: str
            Path to the file

        Returns
        -------
        int
            Size of the file in bytes
        """
        info = self.info(path)
        if info["type"] == "directory":
            return 0
        return info.get("size", 0) or 0  # Default to 0 if size is None

    def sizes(self: Self, paths: list[str]) -> list[int]:
        """Get the size of multiple files

        Parameters
        ----------
        paths: list of str
            Paths to the files

        Returns
        -------
        list of int
            Sizes of the files in bytes
        """
        return [self.size(path) for path in paths]

    def prepare_for_upload(self):
        """Put the artifact in staging mode if it's not already."""
        self._remote_edit(
            version="stage",
        )


async def create_artifact(artifact_id: str, token: str, server_url: str) -> bool:
    """Create a new artifact in Hypha.

    Parameters
    ----------
    artifact_id: str
        The identifier for the new artifact
    token: str
        Authorization token for Hypha
     server_url: str
        The base URL of the Hypha server

    Returns
    -------
    bool
        True if artifact was created successfully, False otherwise
    """
    try:
        # Connect to the Hypha server
        api = await connect_to_server(
            {
                "name": "artifact-client",
                "server_url": server_url.rsplit("/public/services/artifact-manager", 1)[
                    0
                ],
                "token": token,
            }
        )

        # Get the artifact manager service
        artifact_manager = await api.get_service("public/artifact-manager")

        # Create the artifact
        manifest = {
            "name": artifact_id,
            "description": f"Artifact created programmatically: {artifact_id}",
        }

        await artifact_manager.create(
            alias=artifact_id,
            type="generic",
            manifest=manifest,
            config={"permissions": {"*": "r+", "@": "rw+"}},
        )

        # Disconnect from the server
        await api.disconnect()
        return True

    except Exception as e:
        print(f"Failed to create artifact: {e}")
        return False


def create_artifact_sync(artifact_id: str, token: str, server_url: str) -> bool:
    """Synchronous wrapper for create_artifact function"""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(create_artifact(artifact_id, token, server_url))
    finally:
        loop.close()


if __name__ == "__main__":
    import sys
    import signal

    # Set up clean shutdown for asyncio tasks
    def signal_handler(sig, frame):
        print("Shutting down gracefully...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    artifact_id = "example_artifact"
    load_dotenv()  # Load token from .env file
    token = os.getenv("PERSONAL_TOKEN")

    if not token:
        print("ERROR: No PERSONAL_TOKEN found in environment variables")
        print("Please create a .env file with your PERSONAL_TOKEN")
        exit(1)

    # Initialize artifact object
    artifact = HyphaArtifact(artifact_id)
    server_url = artifact.artifact_url

    # Check if the artifact exists by trying to list files
    print(f"Checking if artifact '{artifact_id}' exists...")
    try:
        # Try a simple operation to check if artifact exists and is accessible
        files = artifact.ls("/")
        print(f"Artifact '{artifact_id}' already exists. Found {len(files)} files.")
        artifact_exists = True
    except Exception as e:
        print(
            f"Artifact '{artifact_id}' does not exist or is not accessible. Will try to create it."
        )
        artifact_exists = False

    # Create artifact only if it doesn't exist
    # if not artifact_exists:
    #     print(f"Creating new artifact '{artifact_id}'...")
    #     try:
    #         success = create_artifact_sync(
    #             artifact_id=artifact_id,
    #             token=token,
    #             server_url=server_url,
    #         )
    #         if success:
    #             print(f"Successfully created artifact '{artifact_id}'")
    #         else:
    #             print(f"Failed to create artifact '{artifact_id}'")
    #             sys.exit(1)
    #     except Exception as e:
    #         if "already exists" in str(e):
    #             print(f"Artifact '{artifact_id}' already exists, using it.")
    #         else:
    #             print(f"Error creating artifact: {e}")
    #             sys.exit(1)

    # Clear existing event loops and create a fresh one for our operations
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Proceed with file operations
    print("\n--- File Operations ---")

    # Create test file
    print("Creating test file...")
    try:
        # First, prepare the artifact for upload (put it in staging mode)
        print("Putting artifact in staging mode...")
        artifact.prepare_for_upload()

        # Now try to upload the file
        with artifact.open("/example_file.txt", "w") as f:
            f.write("This is a test file")
        print("✓ Successfully created test file")
    except Exception as e:
        print(f"✗ Failed to create test file: {str(e)}")
        sys.exit(1)

    # List files
    print("\nListing files:")
    try:
        files = artifact.ls("/")
        if files:
            for file in files:
                print(f"- {file.get('name', 'Unknown')}")
        else:
            print("No files found")
        print("✓ Successfully listed files")
    except Exception as e:
        print(f"✗ Failed to list files: {str(e)}")

    # Read file content
    print("\nReading file content:")
    try:
        content = artifact.cat("/example_file.txt")
        print(f"Content: {content}")
        print("✓ Successfully read file content")
    except Exception as e:
        print(f"✗ Failed to read file content: {str(e)}")

    # Copy file
    print("\nCopying file...")
    try:
        artifact.copy("/example_file.txt", "/copy_of_example_file.txt")
        print("✓ Successfully copied file")
    except Exception as e:
        print(f"✗ Failed to copy file: {str(e)}")

    # Check file existence
    print("\nChecking if files exist:")
    try:
        original_exists = artifact.exists("/example_file.txt")
        copy_exists = artifact.exists("/copy_of_example_file.txt")
        print(f"Original file exists: {original_exists}")
        print(f"Copied file exists: {copy_exists}")
        print("✓ Successfully checked file existence")
    except Exception as e:
        print(f"✗ Failed to check file existence: {str(e)}")

    # Remove copied file
    print("\nRemoving copied file...")
    try:
        artifact.rm("/copy_of_example_file.txt")
        print("✓ Successfully removed copied file")
    except Exception as e:
        print(f"✗ Failed to remove copied file: {str(e)}")

    # Final check
    print("\nVerifying final state:")
    try:
        original_exists = artifact.exists("/example_file.txt")
        copy_exists = artifact.exists("/copy_of_example_file.txt")
        print(f"Original file exists: {original_exists}")
        print(f"Copied file exists: {copy_exists} (should be False)")

        if original_exists and not copy_exists:
            print("\n✓ All operations completed successfully!")
        else:
            print("\n✗ Some operations didn't complete as expected.")
    except Exception as e:
        print(f"✗ Failed during final verification: {str(e)}")

    # Clean up any remaining asyncio resources
    pending = asyncio.all_tasks(loop)
    for task in pending:
        task.cancel()

    try:
        # Give tasks a chance to properly cancel
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
    except Exception:
        pass

    print("\nScript completed.")
