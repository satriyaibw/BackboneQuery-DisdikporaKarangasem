def route_tables(tables: dict):
    """Pisah tabel per param_type; sekolah dikecualikan dari npsn & wilayah."""
    tbl_npsn = {t: v for t, v in tables.items()
                if v.get("param_type") == "npsn" and t != "sekolah"}
    tbl_wilayah = {t: v for t, v in tables.items()
                   if v.get("param_type") == "wilayah" and t != "sekolah"}
    tbl_ref = {t: v for t, v in tables.items() if v.get("param_type") == "ref"}
    return tbl_npsn, tbl_wilayah, tbl_ref
