from fastapi import APIRouter

from aggtube.api.v1.api import router as _router

router = APIRouter()
router.include_router(_router)
