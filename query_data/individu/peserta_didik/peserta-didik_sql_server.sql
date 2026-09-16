
/* ------------------------------------------------------------------------------------------------------------------ */
/*                                            Menyajikan Data Peserta Didik                                           */
/* ------------------------------------------------------------------------------------------------------------------ */

/* -------------------------------------------------- MsSQL Server -------------------------------------------------- */

-- Bagian ini menyajikan SELURUH data yang sudah terdaftar pada satuan pendidikan yang sudah terdaftar di registrasi_peserta_didik
-- baik peserta didik yang belum masuk ke rombel maupun sudah masuk ke rombel.
SELECT
a.peserta_didik_id
,a.registrasi_id
,b.rombongan_belajar_id
,b.nisn
,b.nama
,b.tanggal_lahir
,b.tempat_lahir
,b.nama_ibu_kandung
,b.jenis_kelamin
,b.agama
,a.sekolah_id
,b.semester
,b.tingkat_pendidikan
,b.nama_rombel
,b.nik
,b.kode_wilayah
,b.nomor_telepon_seluler
,b.layak_PIP
,b.penerima_KIP
,b.no_KIP
,b.nm_KIP
,b.nik_ibu
,b.nik_ayah
,b.nik_wali
,b.nama_ayah
,b.nama_wali
,b.lintang
,b.bujur
,b.semester_id
,b.alamat_jalan
,b.kewarganegaraan
,b.jenis_rombel
,b.kurikulum_id
FROM backbone_client.dbo.registrasi_peserta_didik a WITH(NOLOCK)
JOIN backbone_client.dbo.sekolah c WITh(NOLOCK)
ON a.sekolah_id = c.sekolah_id AND c.soft_delete = 0
LEFT JOIN (
    -- Bagian ini menyajikan data peserta didik yang aktif di sekolah tertentu yang sudah terdaftar di registrasi_peserta_didik dan sudah terdaftar pada rombongan belajar
	SELECT
	a.peserta_didik_id
	,b.registrasi_id
	,d.rombongan_belajar_id
	,a.nisn
	,a.nama
	,a.tanggal_lahir
	,a.tempat_lahir
	,a.nama_ibu_kandung
	,a.jenis_kelamin
	,f.nama AS agama
	,b.sekolah_id
	,d.semester_id AS semester
	,d.tingkat_pendidikan_id
	,d.nama AS nama_rombel
	,a.nik
	,a.kode_wilayah
	,a.nomor_telepon_seluler
	,a.layak_PIP
	,a.penerima_KIP
	,a.no_KIP
	,a.nm_KIP
	,a.nik_ibu
	,a.nik_ayah
	,a.nik_wali
	,a.nama_ayah
	,a.nama_wali
	,a.lintang
	,a.bujur
	,d.semester_id
	,a.alamat_jalan
	,a.kewarganegaraan
	,d.jenis_rombel
	,d.kurikulum_id
	FROM backbone_client.dbo.peserta_didik a WITH(NOLOCK)
	JOIN backbone_client.dbo.registrasi_peserta_didik b WITH(NOLOCK) ON a.peserta_didik_id = b.peserta_didik_id AND b.Soft_delete = 0 AND b.jenis_keluar_id IS NULL
	JOIN backbone_client.dbo.anggota_rombel c WITH(NOLOCK) ON a.peserta_didik_id = c.peserta_didik_id AND c.Soft_delete = 0
	JOIN backbone_client.dbo.rombongan_belajar d WITH(NOLOCK) ON c.rombongan_belajar_id = d.rombongan_belajar_id  AND b.sekolah_id = d.sekolah_id AND d.jenis_rombel IN (1,8,9,11,12,13,14,22) AND d.Soft_delete = 0 AND d.semester_id = (select top 1 semester_id from backbone_client.ref.semester WITH(NOLOCK) where periode_aktif = 1 order by semester_id desc)
	LEFT JOIN backbone_client.dbo.sekolah e WITH(NOLOCK) ON d.sekolah_id = e.sekolah_id AND b.sekolah_id = e.sekolah_id AND e.bentuk_pendidikan_id NOT IN (24,19,20,21,22,23,66) AND e.Soft_delete = 0
	JOIN backbone_client.ref.agama f WITH(NOLOCK) ON a.agama_id = f.agama_id AND f.expired_date IS NULL
	WHERE  a.soft_delete = 0
) b
ON a.sekolah_id = b.sekolah_id AND a.peserta_didik_id = b.peserta_didik_id
WHERE
a.jenis_keluar_id IS NULL AND a.tanggal_keluar IS NULL
AND a.Soft_delete = 0
-- AND a.sekolah_id = '[sekolah_id]'		-- Digunakan untuk filter data peserta didik berdasarkan sekolah tertentu. Jika ingin menampilkan seluruh data peserta didik, maka baris ini dikomentari.

