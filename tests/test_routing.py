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
