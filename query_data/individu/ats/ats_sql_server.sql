
/* ------------------------------------------------------------------------------------------------------------------ */
/*                                   Menyajikan Data Individu ATS By Name By Address                                  */
/* ------------------------------------------------------------------------------------------------------------------ */


/* ------------------------------------------ Menyajikan Data Individu ATS ------------------------------------------ */
SELECT 
ats.*                                               -- menyajikan semua kolom dari tabel ats
FROM backbone_client.dbo.ats AS ats WITH(NOLOCK)
WHERE
ats.softdelete = 0
AND ats.aktif = 0
AND (
    ats.alasan_approval_id NOT IN (17, 21, 24)      -- FILTER alasan_approval_id NOT IN (17, 21, 24) (17: 'Anak Tidak Ditemukan', 21: 'Meninggal Dunia', 24: 'Sudah Tamat SMA/Sederajat') digunakan untuk mengecualikan individu yang memiliki alasan_approval_id tertentu, sehingga individu tersebut tidak disertakan dalam hasil query
    OR ats.alasan_approval_id IS NULL               -- Filter NULL digunakan jika ada record yang belum terisi alasan_approval_id, sehingga record tersebut tetap disertakan dalam hasil query
)
