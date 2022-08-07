UPDATE surveys_ls
SET
      mag_r_ab = 22.5 - 2.5 * LOG10(flux_r)
    , mag_g_ab = 22.5 - 2.5 * LOG10(flux_g)
    , mag_z_ab = 22.5 - 2.5 * LOG10(flux_z)
    , mag_w1_ab = 22.5 - 2.5 * LOG10(flux_w1)
    , mag_w2_ab = 22.5 - 2.5 * LOG10(flux_w2)
    , mag_w3_ab = 22.5 - 2.5 * LOG10(flux_w3)
    , mag_w4_ab = 22.5 - 2.5 * LOG10(flux_w4)
WHERE 1=1
;