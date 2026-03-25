from fastapi import APIRouter, File, HTTPException, UploadFile, status

router = APIRouter(prefix="/report", tags=["report"])


@router.post("/export", status_code=status.HTTP_501_NOT_IMPLEMENTED)
async def export_report(file: UploadFile = File(...)) -> dict[str, str]:
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file must have a filename.",
        )

    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Report export pipeline is not implemented yet.",
    )
