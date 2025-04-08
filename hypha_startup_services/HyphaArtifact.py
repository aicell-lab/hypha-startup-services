from typing import Literal, Self, overload
from imjoy_rpc.utils import HTTPFile
import requests

type OnError = Literal["raise", "ignore"] | None


class WithHTTPFile(HTTPFile):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __aenter__(self: Self) -> Self:
        return self

    def __aexit__(self: Self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class HyphaArtifact:
    artifact_id: str
    artifact_url: str

    def __init__(self: Self, artifact_id: str):
        self.artifact_id = artifact_id
        self.artifact_url = "https://hypha.aicell.io/public/services/artifact-manager"

    @overload
    def cat(
        self: Self, path: list[str], recursive: bool = False, on_error: OnError = None
    ) -> dict[str, str]: ...

    @overload
    def cat(
        self: Self, path: str, recursive: bool = False, on_error: OnError = None
    ) -> str: ...

    def cat(
        self: Self,
        path: str | list[str],
        recursive: bool = False,
        on_error: OnError = None,
    ) -> dict[str, str] | str: ...

    def open(self: Self, urlpath: str) -> WithHTTPFile: ...

    def copy(
        self: Self,
        path1: str,
        path2: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        on_error: OnError = None,
        **kwargs,
    ): ...

    def cp(self: Self, path1: str, path2: str, **kwargs):
        return self.copy(path1, path2, **kwargs)

    def rm(
        self: Self, path: str, recursive: bool = False, maxdepth: int | None = None
    ): ...

    def created(self: Self, path: str): ...

    def delete(
        self: Self, path: str, recursive: bool = False, maxdepth: int | None = None
    ):
        return self.rm(path, recursive, maxdepth)

    def get(
        self: Self,
        rpath: str,
        lpath: str,
        recursive: bool = False,
        maxdepth: int | None = None,
        callback: callable | None = None,
        **kwargs,
    ): ...

    def get_file(
        self: Self,
        rpath: str,
        lpath: str,
        maxdepth: int | None = None,
        callback: callable | None = None,
        outfile: str | None = None,
        **kwargs,
    ) -> WithHTTPFile: ...

    def download(self: Self, rpath: str, lpath: str, recursive: bool = False, **kwargs):
        return self.get(
            rpath,
            lpath,
            recursive=recursive,
            maxdepth=None,
            callback=None,
            **kwargs,
        )

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
