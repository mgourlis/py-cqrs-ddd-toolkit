import pytest
import sys
from unittest.mock import MagicMock, AsyncMock, patch

# Pre-mock to allow imports
sys.modules["aiofiles"] = MagicMock()
sys.modules["aiofiles.os"] = MagicMock()
sys.modules["aiobotocore"] = MagicMock()
sys.modules["aiobotocore.session"] = MagicMock()
sys.modules["httpx"] = MagicMock()
sys.modules["gcloud"] = MagicMock()
sys.modules["gcloud.aio"] = MagicMock()
sys.modules["gcloud.aio.storage"] = MagicMock()

import cqrs_ddd.backends.storage as storage_mod  # noqa: E402
from cqrs_ddd.backends.storage import (  # noqa: E402
    LocalFileSystemStorage,
    S3Storage,
    DropboxStorage,
    GoogleCloudStorage,
)


# --- Exceptions ---
class S3ClientError(Exception):
    def __init__(self, code="404"):
        self.response = {"Error": {"Code": code}}


# --- Helpers ---
class AsyncCM:
    def __init__(self, val=None):
        self.val = val

    async def __aenter__(self):
        return self.val

    async def __aexit__(self, *args):
        pass


class AIter:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.items:
            raise StopAsyncIteration
        return self.items.pop(0)


# --- LocalFileSystemStorage Tests ---


@pytest.mark.asyncio
async def test_local_storage_comprehensive(monkeypatch):
    monkeypatch.setattr(storage_mod, "HAS_AIOFILES", True)
    mock_aio = MagicMock()
    # path.exists called in: save (1), save cleanup (2), exists (3), read (4)
    # delete (5), delete (6), list (7)
    mock_aio.os.path.exists = AsyncMock(
        side_effect=[False, True, True, True, True, False, True]
    )
    mock_aio.os.remove = AsyncMock(side_effect=[None, FileNotFoundError])
    mock_aio.os.makedirs = AsyncMock()
    mock_aio.os.rename = AsyncMock()
    monkeypatch.setattr(storage_mod, "aiofiles", mock_aio)

    storage = LocalFileSystemStorage(root_path="/tmp")
    m_file = AsyncMock()
    m_file.read.return_value = b"hi"
    with patch("cqrs_ddd.backends.storage.aiofiles.open", return_value=AsyncCM(m_file)):
        await storage.save("f1.txt", b"hi")
        assert await storage.read("f1.txt") == b"hi"
    assert await storage.exists("f1.txt") is True
    await storage.delete("f1.txt")
    await storage.delete("f2.txt")

    with patch(
        "cqrs_ddd.backends.storage.aiofiles.os.listdir", AsyncMock(return_value=["f"])
    ):
        assert await storage.list("") == ["f"]


# --- S3Storage Tests ---


@pytest.mark.asyncio
async def test_s3_storage_comprehensive():
    mock_session = MagicMock()
    mock_client = MagicMock()

    mock_client.head_object = AsyncMock(
        side_effect=[S3ClientError("404"), S3ClientError("500"), {}]
    )
    mock_client.put_object = AsyncMock()
    mock_client.delete_object = AsyncMock()

    m_stream = AsyncMock()
    m_stream.read.return_value = b"hi"
    mock_client.get_object = AsyncMock(return_value={"Body": AsyncCM(m_stream)})

    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = AIter([{"Contents": [{"Key": "/f1"}]}])
    mock_client.get_paginator.return_value = mock_paginator

    mock_session.create_client.return_value = AsyncCM(mock_client)

    storage = S3Storage("b", session=mock_session)
    await storage.save("f1", b"d")
    with pytest.raises(S3ClientError):
        await storage.save("f2", b"d")
    assert await storage.exists("f1") is True

    # read NoSuchKey
    mock_client.get_object.side_effect = S3ClientError("NoSuchKey")
    assert await storage.read("nx") is None

    await storage.delete("f1")
    assert await storage.list("/") == ["/f1"]


# --- DropboxStorage Tests ---


@pytest.mark.asyncio
async def test_dropbox_storage_comprehensive():
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = b"hi"
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch("httpx.AsyncClient", return_value=AsyncCM(mock_client)):
        storage = DropboxStorage("t")
        assert await storage.read("p") == b"hi"
        await storage.save("p", b"d", overwrite=True)

        # 409 conflict on save
        mock_resp.status_code = 409
        mock_resp.json.return_value = {"error": "path_conflict"}
        with pytest.raises(FileExistsError):
            await storage.save("p", b"d")

        # list (reset json return value)
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"entries": [{"name": "f1"}]}
        assert await storage.list("/") == ["f1"]

        # list failure
        mock_resp.status_code = 500
        assert await storage.list("/") == []

        # delete
        mock_resp.status_code = 200
        await storage.delete("p")


# --- GoogleCloudStorage Tests ---


@pytest.mark.asyncio
async def test_gcs_storage_comprehensive():
    mock_client = MagicMock()
    mock_client.download = AsyncMock(return_value=b"hi")
    mock_client.upload = AsyncMock()
    mock_client.delete = AsyncMock()
    mock_client.download_metadata = AsyncMock(return_value={})
    mock_client.list_objects = AsyncMock(return_value=["f1"])

    with patch(
        "gcloud.aio.storage.Storage", return_value=AsyncCM(mock_client), create=True
    ):
        storage = GoogleCloudStorage("b")
        storage.Storage = MagicMock(return_value=AsyncCM(mock_client))

        assert await storage.read("p") == b"hi"
        await storage.save("p", b"d", overwrite=True)

        # overwrite check 404
        mock_client.download.side_effect = Exception("404 not found")
        await storage.save("p2", b"d")

        assert await storage.exists("p") is True
        assert await storage.list("/") == ["f1"]
        await storage.delete("p")

        # errors
        mock_client.download.side_effect = Exception("500")
        with pytest.raises(Exception):
            await storage.read("nx")

        mock_client.download_metadata.side_effect = Exception("fail")
        assert await storage.exists("nx") is False

        mock_client.list_objects.side_effect = Exception("fail")
        assert await storage.list("/") == []
