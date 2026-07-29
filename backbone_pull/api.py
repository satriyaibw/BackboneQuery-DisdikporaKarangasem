import asyncio
from typing import Optional

import aiohttp


class BackboneAPI:
    """Client tipis untuk Backbone API: ambil access-token + GET + retry.

    Access-token (JWT) ditukar dari username/password lewat `fetch_token`; sesudah
    itu `headers` berisi Bearer <token> + X-API-Key untuk semua request data.
    """

    def __init__(self, base_url: str, api_key: str, auth_url: str,
                 username: str, password: str, rate_limiter=None):
        self.base_url = base_url.rstrip("/")
        self.auth_url = auth_url
        self.username = username
        self.password = password
        self.rate_limiter = rate_limiter
        self.base_headers = {"X-API-Key": api_key, "Accept": "application/json"}
        # Belum ada Authorization sampai fetch_token() dipanggil.
        self.headers = dict(self.base_headers)

    async def fetch_token(self, session: aiohttp.ClientSession) -> str:
        """Tukar username/password → access-token, lalu set header Bearer."""
        form = {"username": self.username, "password": self.password}
        headers = {"accept": "application/json",
                   "content-type": "application/x-www-form-urlencoded"}
        async with session.post(self.auth_url, data=form, headers=headers) as resp:
            resp.raise_for_status()
            body = await resp.json()
        token = body.get("access_token")
        if not token:
            raise RuntimeError("Response access-token tidak berisi 'access_token'.")
        self.headers = {**self.base_headers, "Authorization": f"Bearer {token}"}
        return token

    async def get(self, session: aiohttp.ClientSession, path: str,
                  params: dict = None) -> dict:
        if self.rate_limiter is not None:
            await self.rate_limiter.acquire()
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
