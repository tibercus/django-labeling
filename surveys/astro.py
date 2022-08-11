from django.db.models import F
from django.db.models.functions import Log


def mag_ab_from_flux_nanomagies(flux_column: str):
    return 22.5 - 2.5 * Log(10, F(flux_column))
