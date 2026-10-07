
/* ------------------------------------------------------------------------------------------------------------------ */
/*                              Menyajikan Data Yayasan / Badan penyelenggara Pendidikan                              */
/* ------------------------------------------------------------------------------------------------------------------ */

/* -------------------------------------------------- MsSQL Server -------------------------------------------------- */

/* ---------------------------- Menyajikan data yayasan / badan penyelenggara pendidikan ---------------------------- */
SELECT 
yay.*                                           -- menyajikan semua kolom dari tabel yayasan
FROM backbone_client.dbo.yayasan yay WITH(NOLOCK)
WHERE 
yay.soft_delete = 0


/* ----------------------- Satuan Pendidikan yang dinaungi oleh yayasan / badan penyelenggara ----------------------- */
SELECT 
yay.yayasan_id                                                  -- menyajikan semua kolom dari tabel yayasan
,yay.nama as nama_yayasan                                       -- menyajikan nama yayasan dari tabel yayasan
,yay.npyp                                                       -- menyajikan npyp dari tabel yayasan
,sek.*                                                          -- menyajikan semua kolom dari tabel sekolah
FROM Backbone_client.dbo.sekolah sek WITH(NOLOCK) 
JOIN backbone_client.dbo.yayasan yay WITH(NOLOCK) ON sek.id_yayasan = yay.yayasan_id
WHERE 
yay.soft_delete = 0
AND sek.soft_delete = 0
AND sek.keaktifan = 1
