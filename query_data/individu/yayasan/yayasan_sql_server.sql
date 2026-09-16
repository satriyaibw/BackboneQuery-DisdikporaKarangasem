
/* ------------------------------------------------------------------------------------------------------------------ */
/*                              Menyajikan Data Yayasan / Badan penyelenggara Pendidikan                              */
/* ------------------------------------------------------------------------------------------------------------------ */

/* -------------------------------------------------- MsSQL Server -------------------------------------------------- */

SELECT 
yay.*                                           -- menyajikan semua kolom dari tabel yayasan
FROM backbone_client.dbo.yayasan yay WITH(NOLOCK)
WHERE 
yay.soft_delete = 0