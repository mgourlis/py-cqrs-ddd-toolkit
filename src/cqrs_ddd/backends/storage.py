"""Storage backend implementations."""

import os
import asyncio
import uuid
from typing import Optional, List, Any
from ..protocols import StorageService

try:
    import aiofiles
    import aiofiles.os

    HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False


class LocalFileSystemStorage(StorageService):
    """
    Local filesystem implementation of StorageService using aiofiles.

    Features:
    - Atomic writes (write to temp + rename) allows safe concurrent access.
    - Async operations via aiofiles (thread pool optimized).
    """

    def __init__(self, root_path: str = None):
        """
        Initialize storage.

        Args:
            root_path: Optional root directory to confine operations to.
        """
        if not HAS_AIOFILES:
            raise ImportError(
                "aiofiles is required. Install with: pip install aiofiles"
            )

        self.root_path = os.path.abspath(root_path) if root_path else None

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to root_path."""
        if not self.root_path:
            return path

        # Join and normalize
        full_path = os.path.abspath(os.path.join(self.root_path, path))

        # Security check: ensure path is within root
        if not full_path.startswith(self.root_path):
            raise ValueError(f"Path traversal detected: {path}")

        return full_path

    async def save(self, path: str, content: bytes, overwrite: bool = False) -> None:
        """Save content to path atomically using aiofiles."""
        full_path = self._resolve_path(path)

        if not overwrite and await aiofiles.os.path.exists(full_path):
            raise FileExistsError(f"File exists: {path}")

        dir_path = os.path.dirname(full_path)
        await aiofiles.os.makedirs(dir_path, exist_ok=True)

        # Create temp file with unique name in same directory
        tmp_name = f".{os.path.basename(path)}.{uuid.uuid4()}.tmp"
        tmp_path = os.path.join(dir_path, tmp_name)

        try:
            async with aiofiles.open(tmp_path, "wb") as f:
                await f.write(content)

            # Atomic replacement
            await aiofiles.os.rename(tmp_path, full_path)
        except Exception:
            # Cleanup on error
            if await aiofiles.os.path.exists(tmp_path):
                await aiofiles.os.remove(tmp_path)
            raise

    async def read(self, path: str) -> Optional[bytes]:
        """Read content from path using aiofiles."""
        full_path = self._resolve_path(path)

        if not await aiofiles.os.path.exists(full_path):
            return None

        try:
            async with aiofiles.open(full_path, "rb") as f:
                return await f.read()
        except FileNotFoundError:
            return None

    async def delete(self, path: str) -> None:
        """Delete file at path."""
        full_path = self._resolve_path(path)

        try:
            await aiofiles.os.remove(full_path)
        except FileNotFoundError:
            pass

    async def exists(self, path: str) -> bool:
        """Check if file exists."""
        full_path = self._resolve_path(path)
        return await aiofiles.os.path.exists(full_path)

    async def list(self, path: str) -> List[str]:
        """List files in directory."""
        full_path = self._resolve_path(path)

        if not await aiofiles.os.path.exists(full_path):
            return []

        # aiofiles doesn't strictly have listdir in all versions,
        # but modern versions via aiofiles.os should support it wrapped.
        # If not, fallback to run_in_executor
        try:
            # aiofiles.os.listdir matches os.listdir behavior
            return await aiofiles.os.listdir(full_path)
        except AttributeError:
            # Fallback for older versions
            return await asyncio.to_thread(os.listdir, full_path)

    async def get_mime_type(self, path: str) -> Optional[str]:
        """Get MIME type of file using python-magic (content) or mimetypes (name)."""
        full_path = self._resolve_path(path)

        # Try content-based detection first
        try:
            import magic

            # libmagic is synchronous, call in thread pool
            mime_type = await asyncio.to_thread(magic.from_file, full_path, mime=True)
            if mime_type:
                return mime_type
        except (ImportError, Exception):
            pass

        # Fallback to extension-based guessing
        import mimetypes

        mime_type, _ = mimetypes.guess_type(path)
        return mime_type

    async def merge(
        self,
        source_dir: str,
        target_path: str,
        extension: Optional[str] = None,
        delete_source: bool = False,
    ) -> str:
        """Merge chunks from a directory into a single file."""
        full_source_dir = self._resolve_path(source_dir)

        final_filename = os.path.basename(target_path)
        if extension:
            final_filename = f"{final_filename}.{extension.lstrip('.')}"

        full_target_path = self._resolve_path(
            os.path.join(os.path.dirname(target_path), final_filename)
        )

        if not await aiofiles.os.exists(full_source_dir):
            raise FileNotFoundError(f"Source directory not found: {source_dir}")

        entries = await aiofiles.os.listdir(full_source_dir)
        # Assuming chunks are named by their index (0, 1, 2...)
        # We sort them numerically if possible, otherwise lexicographically
        try:
            chunk_files = sorted(entries, key=int)
        except ValueError:
            chunk_files = sorted(entries)

        await aiofiles.os.makedirs(os.path.dirname(full_target_path), exist_ok=True)

        # Write to temp first for atomicity
        tmp_target = f"{full_target_path}.merge.tmp"

        async with aiofiles.open(tmp_target, "wb") as final_file:
            for chunk_file in chunk_files:
                chunk_path = os.path.join(full_source_dir, chunk_file)
                async with aiofiles.open(chunk_path, "rb") as f:
                    # Stream chunk to target
                    while True:
                        chunk_data = await f.read(1024 * 1024)  # 1MB buffer
                        if not chunk_data:
                            break
                        await final_file.write(chunk_data)

        # Atomic move
        await aiofiles.os.rename(tmp_target, full_target_path)

        if delete_source:
            # Recursive delete
            for chunk_file in chunk_files:
                await aiofiles.os.remove(os.path.join(full_source_dir, chunk_file))
            try:
                await aiofiles.os.rmdir(full_source_dir)
            except OSError:
                pass

        # Return relative path if we have root_path, else absolute
        if self.root_path and full_target_path.startswith(self.root_path):
            return os.path.relpath(full_target_path, self.root_path)
        return full_target_path


# =============================================================================
# S3 Storage (AWS)
# =============================================================================


class S3Storage(StorageService):
    """
    S3 implementation of StorageService using aiobotocore.
    """

    def __init__(
        self,
        bucket: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        session: Any = None,
    ):
        try:
            from aiobotocore.session import get_session
        except ImportError:
            raise ImportError(
                "aiobotocore is required for S3Storage. Install with: pip install aiobotocore"
            )

        self.bucket = bucket
        self.session = session or get_session()
        self.config = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "region_name": region_name,
            "endpoint_url": endpoint_url,
        }

    async def save(self, path: str, content: bytes, overwrite: bool = False) -> None:
        async with self.session.create_client("s3", **self.config) as client:
            if not overwrite:
                try:
                    await client.head_object(Bucket=self.bucket, Key=path)
                    raise FileExistsError(f"File exists: {path}")
                except Exception as e:
                    if isinstance(e, FileExistsError):
                        raise
                    # Robust check for 404
                    err_resp = getattr(e, "response", {})
                    code = err_resp.get("Error", {}).get("Code")
                    if code == "404" or "404" in str(e):
                        pass
                    else:
                        raise

            await client.put_object(Bucket=self.bucket, Key=path, Body=content)

    async def read(self, path: str) -> Optional[bytes]:
        async with self.session.create_client("s3", **self.config) as client:
            try:
                response = await client.get_object(Bucket=self.bucket, Key=path)
                async with response["Body"] as stream:
                    return await stream.read()
            except Exception as e:
                err_resp = getattr(e, "response", {})
                code = err_resp.get("Error", {}).get("Code")
                if code == "NoSuchKey" or "NoSuchKey" in str(e):
                    return None
                raise

    async def delete(self, path: str) -> None:
        async with self.session.create_client("s3", **self.config) as client:
            await client.delete_object(Bucket=self.bucket, Key=path)

    async def exists(self, path: str) -> bool:
        async with self.session.create_client("s3", **self.config) as client:
            try:
                await client.head_object(Bucket=self.bucket, Key=path)
                return True
            except Exception as e:
                err_resp = getattr(e, "response", {})
                code = err_resp.get("Error", {}).get("Code")
                if code == "404" or "404" in str(e):
                    return False
                raise

    async def list(self, path: str) -> List[str]:
        async with self.session.create_client("s3", **self.config) as client:
            paginator = client.get_paginator("list_objects_v2")
            prefix = path.lstrip(
                "/"
            )  # S3 doesn't use leading slashes for folders usually
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            files = []
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        # Remove prefix from listing to match ls behavior
                        key = obj["Key"]
                        if key == prefix:
                            continue
                        files.append(key)
            return files

    async def get_mime_type(self, path: str) -> Optional[str]:
        async with self.session.create_client("s3", **self.config) as client:
            try:
                response = await client.head_object(Bucket=self.bucket, Key=path)
                return response.get("ContentType")
            except Exception:
                # Fallback to guessing if head_object fails
                import mimetypes

                mime_type, _ = mimetypes.guess_type(path)
                return mime_type

    async def merge(
        self,
        source_dir: str,
        target_path: str,
        extension: Optional[str] = None,
        delete_source: bool = False,
    ) -> str:
        """
        Merge chunks in S3 using server-side copy (zero-download).

        This uses Multipart Upload + UploadPartCopy to combine objects
        directly within S3 without downloading them to the application server.
        """
        chunks = await self.list(source_dir)
        if not chunks:
            raise FileNotFoundError(f"No chunks found in {source_dir}")

        # Sort numerically if possible
        try:
            sorted_chunks = sorted(chunks, key=lambda x: int(os.path.basename(x)))
        except ValueError:
            sorted_chunks = sorted(chunks)

        final_key = target_path
        if extension:
            final_key = f"{final_key}.{extension.lstrip('.')}"

        async with self.session.create_client("s3", **self.config) as client:
            # 1. Initiate Multipart Upload
            response = await client.create_multipart_upload(
                Bucket=self.bucket, Key=final_key
            )
            upload_id = response["UploadId"]

            parts = []
            try:
                # 2. Copy parts server-side
                for i, chunk_key in enumerate(sorted_chunks, start=1):
                    # S3 CopySource must include bucket
                    copy_source = f"{self.bucket}/{chunk_key}"

                    part_resp = await client.upload_part_copy(
                        Bucket=self.bucket,
                        Key=final_key,
                        CopySource=copy_source,
                        PartNumber=i,
                        UploadId=upload_id,
                    )
                    parts.append(
                        {"PartNumber": i, "ETag": part_resp["CopyPartResult"]["ETag"]}
                    )

                # 3. Complete Upload
                await client.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=final_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )

                # 4. Efficient Batch Delete source chunks
                if delete_source:
                    # S3 can delete up to 1000 objects in one go
                    delete_keys = [{"Key": k} for k in sorted_chunks]
                    await client.delete_objects(
                        Bucket=self.bucket,
                        Delete={"Objects": delete_keys, "Quiet": True},
                    )
            except Exception:
                # Cleanup on failure
                await client.abort_multipart_upload(
                    Bucket=self.bucket, Key=final_key, UploadId=upload_id
                )
                raise

        return final_key


# =============================================================================
# Dropbox Storage
# =============================================================================


class DropboxStorage(StorageService):
    """
    Dropbox implementation using aiohttp to query the API v2.
    """

    def __init__(self, access_token: str, root_path: str = None):
        """
        Initialize Dropbox storage.

        Args:
            access_token: Dropbox API access token.
            root_path: Optional "bucket" folder to confine operations to.
        """
        try:
            import httpx

            self.httpx = httpx
        except ImportError:
            raise ImportError(
                "httpx is required for DropboxStorage. Install with: pip install httpx"
            )

        self.access_token = access_token
        self.root_path = root_path
        self.api_base = "https://content.dropboxapi.com/2"
        self.rpc_base = "https://api.dropboxapi.com/2"

    def _headers(self, content_type: str = "application/json") -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": content_type,
        }

    def _resolve_path(self, path: str) -> str:
        """Resolve path relative to root_path."""
        # Normalize slashes
        path = path.strip("/")

        if self.root_path:
            # Join with root_path
            root = self.root_path.strip("/")
            full_path = f"/{root}/{path}"
        else:
            full_path = f"/{path}"

        return full_path

    async def save(self, path: str, content: bytes, overwrite: bool = False) -> None:
        mode = "overwrite" if overwrite else "add"
        full_path = self._resolve_path(path)

        args = {"path": full_path, "mode": mode, "autorename": False, "mute": True}

        import json

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": json.dumps(args),
        }

        async with self.httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.api_base}/files/upload", headers=headers, content=content
            )
            if resp.status_code == 409:
                error = resp.json()
                if "conflict" in str(error):
                    raise FileExistsError(f"File exists: {path}")
            resp.raise_for_status()

    async def read(self, path: str) -> Optional[bytes]:
        full_path = self._resolve_path(path)
        import json

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Dropbox-API-Arg": json.dumps({"path": full_path}),
        }

        async with self.httpx.AsyncClient() as client:
            resp = await client.post(f"{self.api_base}/files/download", headers=headers)
            if resp.status_code == 409:  # Path error
                return None
            if resp.status_code == 200:
                return resp.content
            return None

    async def delete(self, path: str) -> None:
        full_path = self._resolve_path(path)
        async with self.httpx.AsyncClient() as client:
            payload = {"path": full_path}
            resp = await client.post(
                f"{self.rpc_base}/files/delete_v2",
                headers=self._headers(),
                json=payload,
            )
            if resp.status_code == 409:
                pass  # Ignore if missing

    async def exists(self, path: str) -> bool:
        full_path = self._resolve_path(path)
        async with self.httpx.AsyncClient() as client:
            payload = {"path": full_path}
            resp = await client.post(
                f"{self.rpc_base}/files/get_metadata",
                headers=self._headers(),
                json=payload,
            )
            return resp.status_code == 200

    async def list(self, path: str) -> List[str]:
        full_path = self._resolve_path(path)
        if full_path == "/":
            full_path = ""  # Dropbox root listing uses empty string

        async with self.httpx.AsyncClient() as client:
            payload = {"path": full_path, "recursive": False}
            resp = await client.post(
                f"{self.rpc_base}/files/list_folder",
                headers=self._headers(),
                json=payload,
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            return [entry["name"] for entry in data.get("entries", [])]

    async def get_mime_type(self, path: str) -> Optional[str]:
        # Dropbox get_metadata doesn't return MIME type usually.
        # Fallback to guessing from extension.
        import mimetypes

        mime_type, _ = mimetypes.guess_type(path)
        return mime_type

    async def merge(
        self,
        source_dir: str,
        target_path: str,
        extension: Optional[str] = None,
        delete_source: bool = False,
    ) -> str:
        """
        Merge chunks in Dropbox.
        Downloads all chunks, concatenates them, and uploads the final result.
        """
        chunks = await self.list(source_dir)
        # Sort numerically if possible
        try:
            sorted_chunks = sorted(chunks, key=lambda x: int(os.path.basename(x)))
        except ValueError:
            sorted_chunks = sorted(chunks)

        final_path = target_path
        if extension:
            final_path = f"{final_path}.{extension.lstrip('.')}"

        combined_content = b""
        for chunk_name in sorted_chunks:
            # list() returns just names, we need full path relative to root_path
            chunk_path = os.path.join(source_dir, chunk_name)
            content = await self.read(chunk_path)
            if content:
                combined_content += content

        await self.save(final_path, combined_content, overwrite=True)

        if delete_source:
            # In Dropbox, deleting the directory is atomic and efficient
            await self.delete(source_dir)

        return final_path


# =============================================================================
# Google Cloud Storage (GCS)
# =============================================================================


class GoogleCloudStorage(StorageService):
    """
    GCS implementation using gcloud-aio-storage.
    """

    def __init__(self, bucket: str, service_file: str = None, token: str = None):
        try:
            from gcloud.aio.storage import Storage

            self.Storage = Storage
        except ImportError:
            raise ImportError(
                "gcloud-aio-storage is required. Install with: pip install gcloud-aio-storage"
            )

        self.bucket = bucket
        self.service_file = service_file
        self.token = token

    async def save(self, path: str, content: bytes, overwrite: bool = False) -> None:
        async with self.Storage(
            service_file=self.service_file, token=self.token
        ) as client:
            if not overwrite:
                try:
                    await client.download(self.bucket, path)
                    raise FileExistsError(f"File exists: {path}")
                except Exception as e:
                    if "404" not in str(e):
                        raise

            await client.upload(self.bucket, path, content)

    async def read(self, path: str) -> Optional[bytes]:
        async with self.Storage(
            service_file=self.service_file, token=self.token
        ) as client:
            try:
                return await client.download(self.bucket, path)
            except Exception as e:
                # gcloud-aio raises generic exceptions often, check string
                if "404" in str(e):
                    return None
                raise

    async def delete(self, path: str) -> None:
        async with self.Storage(
            service_file=self.service_file, token=self.token
        ) as client:
            try:
                await client.delete(self.bucket, path)
            except Exception:
                pass

    async def exists(self, path: str) -> bool:
        # GCS doesn't have a cheap 'exists' check in aio lib usually, try download metadata or list
        # We can implement via list (expensive) or exception catching on metadata
        async with self.Storage(
            service_file=self.service_file, token=self.token
        ) as client:
            try:
                await client.download_metadata(self.bucket, path)
                return True
            except Exception:
                return False

    async def list(self, path: str) -> List[str]:
        async with self.Storage(
            service_file=self.service_file, token=self.token
        ) as client:
            prefix = path.lstrip("/")
            # gcloud-aio-storage list_objects return list of names
            try:
                keys = await client.list_objects(self.bucket, prefix=prefix)
                return keys
            except Exception:
                return []

    async def get_mime_type(self, path: str) -> Optional[str]:
        async with self.Storage(
            service_file=self.service_file, token=self.token
        ) as client:
            try:
                metadata = await client.download_metadata(self.bucket, path)
                return metadata.get("contentType")
            except Exception:
                import mimetypes

                mime_type, _ = mimetypes.guess_type(path)
                return mime_type

    async def merge(
        self,
        source_dir: str,
        target_path: str,
        extension: Optional[str] = None,
        delete_source: bool = False,
    ) -> str:
        """
        Merge chunks in GCS using server-side compose (zero-download).

        This uses the GCS Compose API to combine up to 32 objects at a time
        natively within Google Cloud Storage.
        """
        chunks = await self.list(source_dir)
        if not chunks:
            raise FileNotFoundError(f"No chunks found in {source_dir}")

        # Sort numerically if possible
        try:
            sorted_chunks = sorted(
                chunks, key=lambda x: int(os.path.basename(x).split("/")[-1])
            )
        except ValueError:
            sorted_chunks = sorted(chunks)

        final_path = target_path
        if extension:
            final_path = f"{final_path}.{extension.lstrip('.')}"

        async with self.Storage(
            service_file=self.service_file, token=self.token
        ) as client:
            # GCS Compose supports up to 32 source objects.
            MAX_COMPOSE_SOURCES = 32

            if len(sorted_chunks) <= MAX_COMPOSE_SOURCES:
                await client.compose(self.bucket, sorted_chunks, final_path)
            else:
                # Multi-batch compose for more than 32 chunks
                current_batch = sorted_chunks[:MAX_COMPOSE_SOURCES]
                await client.compose(self.bucket, current_batch, final_path)

                remaining = sorted_chunks[MAX_COMPOSE_SOURCES:]
                while remaining:
                    # In subsequent calls, include the current final_path as the first source
                    batch = remaining[: MAX_COMPOSE_SOURCES - 1]
                    await client.compose(self.bucket, [final_path] + batch, final_path)
                    remaining = remaining[MAX_COMPOSE_SOURCES - 1 :]

            if delete_source:
                # GCS composed objects usually need their sources deleted.
                # If source_dir is used as a prefix, we should list and delete everything.
                # For now, deleting the specific chunks known to us.
                for chunk_key in sorted_chunks:
                    await client.delete(self.bucket, chunk_key)

                # Try to delete the directory-like object if it exists (some tools create it)
                try:
                    await client.delete(self.bucket, source_dir.rstrip("/") + "/")
                except Exception:
                    pass

        return final_path
