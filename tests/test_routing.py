from backbone_pull.routing import route_tables


def test_route_splits_by_param_type():
    tables = {
        "sekolah": {"param_type": "wilayah"},
        "guru": {"param_type": "npsn"},
        "rombel": {"param_type": "wilayah"},
        "ref_agama": {"param_type": "ref"},
    }
    npsn, wilayah, ref = route_tables(tables)
    assert set(npsn) == {"guru"}
    assert set(wilayah) == {"rombel"}          # sekolah dikecualikan
    assert set(ref) == {"ref_agama"}
    assert "sekolah" not in npsn and "sekolah" not in wilayah


def test_route_uses_primary_param_type_for_comma_list():
    # Tabel dual-access (mis. dbo.ats, dbo.peserta_didik): server sekarang
    # kirim param_type sebagai comma-list ("npsn,wilayah") langsung, bukan
    # nilai tunggal + kolom param_types_available terpisah. Bucket
    # (tbl_npsn/tbl_wilayah/tbl_ref) tetap harus pakai param_type UTAMA
    # (elemen pertama) supaya tabel itu tidak masuk dua bucket sekaligus --
    # jalur "extra" wilayah tetap ditangani terpisah di flow.py lewat
    # param_types_available (lihat backbone_client_pull()).
    tables = {
        "ats": {"param_type": "npsn,wilayah",
                "param_types_available": {"npsn", "wilayah"}},
        "guru": {"param_type": "npsn"},
    }
    npsn, wilayah, ref = route_tables(tables)
    assert set(npsn) == {"ats", "guru"}
    assert set(wilayah) == set()
