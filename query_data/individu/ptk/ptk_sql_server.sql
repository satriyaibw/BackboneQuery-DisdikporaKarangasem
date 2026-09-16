
/* ------------------------------------------------------------------------------------------------------------------ */
/*                                                 Menyajikan Data PTK                                                */
/* ------------------------------------------------------------------------------------------------------------------ */

/* -------------------------------------------------- MsSQL Server -------------------------------------------------- */

SELECT 
p.*             -- Menyajikan semua variabel identitas di tabel peserta_didik
,pt.*           -- Menyajikan informasi keaktifan ptk sesuai penugasan di satuan pendidikan
,sek.*          -- Menyajikan informasi satuan pendidikan dimana ptk ditugaskan

FROM backbone_client.dbo.ptk AS p WITH (NOLOCK) 
JOIN backbone_client.dbo.ptk_terdaftar AS pt WITH (NOLOCK) ON p.ptk_id = pt.ptk_id 
JOIN backbone_client.dbo.sekolah AS sek WITH (NOLOCK) ON sek.sekolah_id = pt.sekolah_id
WHERE 
p.soft_delete = 0
AND pt.soft_delete = 0
AND sek.soft_delete = 0
AND pt.ptk_induk = 1                                    -- Filter ptk_induk = 1 berarti PTK tersebut merupakan PTK induk di satuan pendidikan, jika ptk_iduk = 0 dinyatakan PTK tersebut merupakan PTK dengan penugasan tambahan di satuan pendidikan
AND pt.jenis_keluar_id IS NULL                          -- Filter NULL pada ptk_terdaftar berarti PTK tersebut masih aktif di satuan pendidikan, jika sudah terisi dinyatakan sudah tidak aktif di satuan pendidikan tersebut
AND pt.tgl_ptk_keluar IS NULL                           -- Filter NULL pada ptk_terdaftar berarti PTK tersebut masih aktif di satuan pendidikan, jika sudah terisi dinyatakan sudah tidak aktif di satuan pendidikan tersebut
AND (pt.tahun_ajaran_id = (SELECT TOP (1) tahun_ajaran_id
                               FROM backbone_client.ref.tahun_ajaran WITH (NOLOCK)
                               WHERE        (periode_aktif = 1)
                               ORDER BY tahun_ajaran_id DESC))

