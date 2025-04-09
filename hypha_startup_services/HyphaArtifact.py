from typing import Literal, Self, overload
from imjoy_rpc.utils import HTTPFile
import requests

type OnError = Literal["raise", "ignore"]
type JsonType = str | int | float | bool | None | dict[str, "JsonType"] | list[
    "JsonType"
]
type FileMode = Literal["r", "rb", "w", "wb", "a", "ab"]
type UploadMethod = Literal["POST", "PUT"]


class WithHTTPFile(HTTPFile):
    def __init__(
        self: Self,
        url: str,
        mode: FileMode = "r",
        encoding: str | None = None,
        newline: str | None = None,
        name: str | None = None,
        upload_method: UploadMethod = "POST",
    ):
        super().__init__(
            url=url,
            mode=mode,
            encoding=encoding,
            newline=newline,
            name=name,
            upload_method=upload_method,
        )

    def __aenter__(self: Self) -> Self:
        return self

    def __aexit__(self: Self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class HyphaArtifact:
    artifact_id: str
    artifact_url: str
    token: str
    workspace_id: str

    def __init__(self: Self, artifact_id: str):
        self.artifact_id = artifact_id
        self.token = "test-token"
        self.workspace_id = "test-workspace-id"
        self.artifact_url = "https://hypha.aicell.io/public/services/artifact-manager"

    def _extend_params(
        self: Self,
        params: dict[str, JsonType],
    ) -> dict[str, JsonType]:
        params["artifact_id"] = f"{self.workspace_id}/{self.artifact_id}"
        params["token"] = self.token

        return params

    def _remote_post(self: Self, method_name: str, params: dict[str, JsonType]) -> None:
        extended_params = self._extend_params(params)
        return requests.post(
            f"{self.artifact_url}/{method_name}",
            params=extended_params,
            timeout=20,
        )

    def _remote_get(self: Self, method_name: str, params: dict[str, JsonType]) -> None:
        extended_params = self._extend_params(params)
        return requests.get(
            f"{self.artifact_url}/{method_name}", params=extended_params, timeout=20
        )

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
            "download_weight": download_weight,
        }
        return self._remote_post("put_file", params)

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
        on_error: OnError = None,
    ) -> dict[str, str] | str: ...

    def open(self: Self, urlpath: str) -> WithHTTPFile:
        download_url = self._remote_get_file(urlpath)

        return WithHTTPFile(
            url=download_url,
            mode="rb",
            name=urlpath,
            upload_method="PUT",
        )

    def copy(
        self: Self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        on_error: OnError | None = None,
        **kwargs,
    ): ...

    def cp(
        self: Self, path1: str, path2: str, on_error: OnError | None = None, **kwargs
    ):
        return self.copy(path1, path2, on_error=on_error, **kwargs)

    def rm(
        self: Self, path: str, recursive: bool = False, maxdepth: int | None = None
    ) -> None:
        return self._remote_remove_file(path)

    def created(self: Self, path: str): ...

    def delete(
        self: Self, path: str, recursive: bool = False, maxdepth: int | None = None
    ):
        return self.rm(path, recursive, maxdepth)

    def exists(self: Self, path: str, **kwargs) -> bool: ...

    def find(
        self: Self,
        path: str,
        maxdepth: int | None = None,
        withdirs: bool = False,
        detailed: bool = False,
        **kwargs,
    ) -> list[str]: ...

    def head(self: Self, path: str, size: int = 1024) -> bytes: ...

    def info(self: Self, path: str, **kwargs) -> dict[str, str | int]: ...

    def isdir(self: Self, path: str) -> bool: ...

    def isfile(self: Self, path: str) -> bool: ...

    def listdir(self: Self, path: str, **kwargs) -> list[str]:
        return self.ls(path, **kwargs)

    def ls(
        self: Self, path: str, detail: bool = True, **kwargs
    ) -> list[str | dict]: ...

    def mkdir(self: Self, path: str, create_parents: bool = True, **kwargs) -> None: ...

    def makedirs(self: Self, path: str, exist_ok: bool = True, **kwargs) -> None: ...

    def makedir(self: Self, path: str, create_parents: bool = True, **kwargs) -> None:
        return self.mkdir(path, create_parents, **kwargs)

    def rm_file(self: Self, path: str): ...

    def rmdir(self: Self, path: str): ...

    def size(self: Self, path: str): ...

    def sizes(self: Self, paths: list[str]): ...
