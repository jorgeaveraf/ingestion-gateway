import csv
from typing import Iterable, List

from fastapi import HTTPException, UploadFile, status

ALLOWED_EXTENSIONS = {".csv"}

def _is_csv_filename(filename: str) -> bool:
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in ALLOWED_EXTENSIONS)

async def validate_csv_files(files: Iterable[UploadFile]) -> List[UploadFile]:
    valid_files: List[UploadFile] = []

    for upload in files:
        if not _is_csv_filename(upload.filename or ""):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File {upload.filename!r} must have a .csv extension.",
            )

        try:
            # Read a small sample only (DON'T read entire file)
            sample_bytes = await upload.read(4096)
            upload.file.seek(0)

            text = sample_bytes.decode("utf-8-sig", errors="replace")
            # Normalize newlines to reduce sniffer flakiness
            text = text.replace("\r\n", "\n").replace("\r", "\n")

            if text.strip():
                try:
                    csv.Sniffer().sniff(text, delimiters=[",", ";", "\t", "|"])
                except Exception:
                    # Fallback: assume comma and validate it yields multiple columns
                    reader = csv.reader(text.splitlines(), delimiter=",")
                    first_row = next(reader, [])
                    if len(first_row) <= 1:
                        raise
        except Exception as exc:
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
