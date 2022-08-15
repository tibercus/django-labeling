from django.db.models import F
from django.db.models.functions import Log, Ln, Sqrt


def mag_ab_from_flux_nanomagies(flux_column: str):
    return 22.5 - 2.5 * Log(10, F(flux_column))


def mag_err_ab_from_flux_nanomaggies(flux_ivar_column: str, flux_column: str):
    # TODO оошибки получаются отрицательные ??? просто убрать минус?
    return -2.5 / (Ln(10) * F(flux_column) * Sqrt(F(flux_ivar_column)))
