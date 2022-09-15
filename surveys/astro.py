"""A module implementing astronomical units conversions via Django ORM."""

from django.db.models import F
from django.db.models.expressions import CombinedExpression
from django.db.models.functions import Log, Ln, Sqrt


def mag_ab_from_flux_nanomagies(flux_column: str) -> CombinedExpression:
    """Calculate AB-magnitude from flux (nanomaggies)."""
    return 22.5 - 2.5 * Log(10, F(flux_column))


def mag_err_ab_from_flux_nanomaggies(
        flux_column: str, flux_ivar_column: str) -> CombinedExpression:
    """Calculate AB-magnitude error from flux (nanomaggies) and flux ivar"""
    # TODO оошибки получаются отрицательные ??? просто убрать минус?
    return 2.5 / (Ln(10) * F(flux_column) * Sqrt(F(flux_ivar_column)))


def mag_ab_from_flux_jy(flux_column: str) -> CombinedExpression:
    """Calculate AB-magnitude from flux (Jy).

    Formula taken from here
    https://iopscience.iop.org/article/10.1088/0004-637X/750/2/99
    """
    return - 2.5 * Log(10, F(flux_column) / 3631)


def mag_err_ab_from_flux_jy(
        flux_column: str, flux_err_column: str) -> CombinedExpression:
    """Calculate AB-magnitude error from flux (Jy) and flux error."""
    return 2.5 * F(flux_err_column) / (Ln(10) * F(flux_column))
