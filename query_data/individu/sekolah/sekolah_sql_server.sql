
/* ------------------------------------------------------------------------------------------------------------------ */
/*                                               Menyajikan Data Sekolah                                              */
/* ------------------------------------------------------------------------------------------------------------------ */

/* -------------------------------------------------- MsSQL Server -------------------------------------------------- */

SELECT 
sek.*                                           -- menyajikan semua kolom dari tabel sekolah
,yay.yayasan_id                                 -- menyajikan yayasan_id dari tabel yayasan untuk satuan pendidikan yang memiliki yayasan
,yay.nama as nama_yayasan                       -- menyajikan nama yayasan dari tabel yayasan untuk satuan pendidikan yang memiliki yayasan
,bp.nama as bentuk_pendidikan                   -- menyajikan nama bentuk pendidikan dari tabel referensi bentuk_pendidikan
,prov.kode_wilayah as kode_provinsi             -- menyajikan kode wilayah provinsi dari tabel referensi mst_wilayah
,prov.nama as nama_provinsi                     -- menyajikan nama provinsi dari tabel referensi mst_wilayah
,kab.kode_wilayah as kode_kabupaten             -- menyajikan kode wilayah kabupaten dari tabel referensi mst_wilayah
,kab.nama as nama_kabupaten                     -- menyajikan nama kabupaten dari tabel referensi mst_wilayah
,kec.kode_wilayah as kode_kecamatan             -- menyajikan kode wilayah kecamatan dari tabel referensi mst_wilayah
,kec.nama as nama_kecamatan                     -- menyajikan nama kecamatan dari tabel referensi mst_wilayah
,desa.kode_wilayah as kode_desa                 -- menyajikan kode wilayah desa dari tabel referensi mst_wilayah
,desa.nama as nama_desa                         -- menyajikan nama desa dari tabel referensi mst_wilayah
FROM backbone_client.dbo.sekolah sek WITH(NOLOCK)
JOIN backbone_client.ref.bentuk_pendidikan bp WITH(NOLOCK) ON sek.bentuk_pendidikan_id = bp.bentuk_pendidikan_id
LEFT JOIN backbone_client.dbo.yayasan yay WITH(NOLOCK) ON sek.yayasan_id = yay.yayasan_id AND yay.soft_delete = 0 
LEFT JOIN backbone_client.ref.mst_wilayah desa WITH(NOLOCK) ON a.kode_wilayah = desa.kode_wilayah AND desa.id_level_wilayah = 4 AND desa.expired_date IS NULL
LEFT JOIN backbone_client.ref.mst_wilayah kec WITH(NOLOCK) ON LEFT(a.kode_wilayah,6) = LEFT(kec.kode_wilayah,6) AND kec.id_level_wilayah = 3 AND kec.expired_date IS NULL
LEFT JOIN backbone_client.ref.mst_wilayah kab WITH(NOLOCK) ON LEFT(a.kode_wilayah,4) = LEFT(kab.kode_wilayah,4) AND kab.id_level_wilayah = 2 AND kab.expired_date IS NULL
LEFT JOIN backbone_client.ref.mst_wilayah prov WITH(NOLOCK) ON LEFT(a.kode_wilayah,2) = LEFT(prov.kode_wilayah,2) AND prov.id_level_wilayah = 1 AND prov.expired_date IS NULL
WHERE 
AND sek.soft_delete = 0 
AND sek.keaktifan = 1

