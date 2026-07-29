import asyncio
from typing import Optional

import aiohttp


class BackboneAPI:
    """Client tipis untuk Backbone API: header tetap + GET + retry."""

    def __init__(self, base_url: str, api_key: str, service_jwt: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {service_jwt}",
            "X-API-Key": api_key,
            "Accept": "application/json",
        }

    async def get(self, session: aiohttp.ClientSession, path: str,
                  params: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        async with session.get(url, params=params, headers=self.headers) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_with_retry(self, session, path, params, label, logger,
                             max_retries: int = 3) -> Optional[dict]:
        for attempt in range(max_retries):
            try:
                return await self.get(session, path, params)
            except Exception as e:  # noqa: BLE001
                if attempt < max_retries - 1:
                    wait = 5 * (3 ** attempt)  # 5s -> 15s
                    logger.warning(
                        f"{label} attempt {attempt + 1}/{max_retries} gagal, retry {wait}s: {e}"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.warning(f"{label} gagal setelah {max_retries}x retry: {e}")
        return None

    @staticmethod
    def is_sp_error(data: list) -> bool:
        return bool(data and "keterangan" in data[0])
