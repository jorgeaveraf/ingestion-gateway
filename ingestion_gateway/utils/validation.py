import csv
from typing import Iterable, List

from fastapi import HTTPException, UploadFile, status

ALLOWED_EXTENSIONS = {".csv"}


def _is_csv_filename(filename: str) -> bool:
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in ALLOWED_EXTENSIONS)


async def validate_csv_files(files: Iterable[UploadFile]) -> List[UploadFile]:
    """
    Perform basic sanity checks on uploaded files to ensure they are CSV-like.
    - correct extension
    - readable small sample
    """
    valid_files = []
    for upload in files:
        if not _is_csv_filename(upload.filename or ""):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File {upload.filename!r} must have a .csv extension.",
            )
        try:
            # Read a small sample to ensure it's parseable
            sample = (await upload.read())[:1024]
            upload.file.seek(0)
            # Attempt parsing a couple rows to verify structure
            text = sample.decode("utf-8", errors="replace")
            csv.Sniffer().sniff(text) if text.strip() else None
        except Exception as exc:  # broad to return user-friendly validation error
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File {upload.filename!r} is not readable as CSV: {exc}",
            ) from exc
        valid_files.append(upload)
    if not valid_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one CSV file is required.",
        )
    return valid_files
