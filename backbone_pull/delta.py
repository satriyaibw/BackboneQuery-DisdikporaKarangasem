"""Delta discovery: persempit daftar NPSN ke yang benar-benar berubah sejak
last_update, sebelum backbone_pull.flow.pull_by_npsn menjalankan loop
per-NPSN yang sudah ada. Loop NPSN itu sendiri tidak berubah -- fungsi ini
cuma mempersempit daftar yang masuk ke loop tersebut."""

from typing import List, Optional


async def resolve_npsn_list(api, session, tbl_name: str, last_update: Optional[str],
                            npsn_list: List[str], logger) -> List[str]:
    if not last_update:
        return npsn_list

    result = await api.get_with_retry(
        session, "/data/npsn-changed",
        {"tbl_name": tbl_name, "last_update": last_update},
        f"{tbl_name}/npsn-changed", logger)

    if result is None:
        logger.warning(
            f"{tbl_name}: delta discovery gagal (tidak ada respons), "
            f"fallback ke {len(npsn_list)} NPSN penuh")
        return npsn_list

    data = result.get("data", [])
    if api.is_sp_error(data):
        logger.warning(
            f"{tbl_name}: delta discovery sp_error ({data[0].get('keterangan')}), "
            f"fallback ke {len(npsn_list)} NPSN penuh")
        return npsn_list

    changed = {row["npsn"] for row in data if row.get("npsn")}
    narrowed = [n for n in npsn_list if n in changed]
    logger.info(f"{tbl_name}: delta discovery mempersempit {len(npsn_list)} -> {len(narrowed)} NPSN")
    return narrowed
