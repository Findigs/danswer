import os
import shutil
import time
import uuid
from pathlib import Path
from typing import Any
from typing import IO

from cloudstorage.drivers.amazon import S3Driver

from danswer.configs.app_configs import FILE_CONNECTOR_TMP_STORAGE_PATH
from danswer.configs.app_configs import AWS_ACCESS_KEY_ID
from danswer.configs.app_configs import AWS_SECRET_ACCESS_KEY
from danswer.configs.app_configs import AWS_DEFAULT_REGION
from danswer.utils.logger import setup_logger

logger = setup_logger()

_VALID_FILE_EXTENSIONS = [".txt", ".zip", ".pdf", ".md", ".mdx"]

storage = S3Driver(
    key=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
    region=AWS_DEFAULT_REGION,
)
container = storage.get_container("danswer")


def get_file_ext(file_path_or_name: str | Path) -> str:
    _, extension = os.path.splitext(file_path_or_name)
    return extension


def check_file_ext_is_valid(ext: str) -> bool:
    return ext in _VALID_FILE_EXTENSIONS


def write_temp_files(
    files: list[tuple[str, IO[Any]]],
    base_path: Path | str = FILE_CONNECTOR_TMP_STORAGE_PATH,
) -> list[str]:
    """Writes temporary files to disk and returns their paths

    NOTE: need to pass in (file_name, File) tuples since FastAPI's `UploadFile` class
    exposed SpooledTemporaryFile does not include a name.
    """
    file_uuid = str(uuid.uuid4())
    file_location = Path(base_path) / file_uuid
    os.makedirs(file_location, exist_ok=True)

    file_paths: list[str] = []
    for file_name, file in files:
        extension = get_file_ext(file_name)
        if not check_file_ext_is_valid(extension):
            raise ValueError(
                f"Invalid file extension for file: '{file_name}'. Must be one of {_VALID_FILE_EXTENSIONS}"
            )

        file_path = file_location / file_name
        with open(file_path, "wb") as buffer:
            # copy file content from uploaded file to the newly created file
            shutil.copyfileobj(file, buffer)
            # write to S3Driver
            file.seek(0)
            container.upload_blob(file, blob_name=file_uuid + "_" + file_name)

        file_paths.append(str(file_path.absolute()))
    return file_paths


def file_age_in_hours(filepath: str | Path) -> float:
    return (time.time() - os.path.getmtime(filepath)) / (60 * 60)
