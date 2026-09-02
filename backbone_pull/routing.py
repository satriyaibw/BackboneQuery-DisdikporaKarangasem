def _primary_param_type(v: dict) -> str:
    """Tabel dual-access (mis. dbo.ats: param_type='npsn,wilayah') dibucket
    pakai param_type UTAMA (elemen pertama comma-list) -- jalur wilayah
    tambahannya ditangani terpisah lewat param_types_available, bukan lewat
    bucket ini (lihat backbone_client_pull() di flow.py)."""
    return v.get("param_type", "").split(",", 1)[0].strip()


def route_tables(tables: dict):
    """Pisah tabel per param_type utama; sekolah dikecualikan dari npsn & wilayah."""
    tbl_npsn = {t: v for t, v in tables.items()
                if _primary_param_type(v) == "npsn" and t != "sekolah"}
    tbl_wilayah = {t: v for t, v in tables.items()
                   if _primary_param_type(v) == "wilayah" and t != "sekolah"}
    tbl_ref = {t: v for t, v in tables.items() if _primary_param_type(v) == "ref"}
    return tbl_npsn, tbl_wilayah, tbl_ref
