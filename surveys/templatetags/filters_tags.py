from typing import Union

from django import template
from django.core.files.storage import default_storage
from ..models import *

import surveys.models as sm

from django.conf import settings
from django.db.models import Model
import os

import astropy.units as u
from astropy.coordinates import SkyCoord

register = template.Library()


@register.simple_tag
def define(val=None):
    """define variable with value"""
    return val


@register.filter
def to_str(value):
    """converts int to string"""
    return str(value)


@register.filter
def is_in_survey(sources, survey):
    """add filter option in template"""
    try:
        source = sources.get(survey=survey)
    except eROSITA.DoesNotExist:
        source = None
    return source


@register.filter
def get_master_source(meta_object: MetaObject) -> eROSITA:
    """add filter option in template"""
    try:
        sources = meta_object.object_sources.all()
        master_source = sources.get(survey__name=meta_object.master_survey)
    except eROSITA.DoesNotExist:
        master_source = None
    return master_source


@register.filter()
def get_opt_source(master_source: eROSITA, survey: str
                   ) -> Union[LS, PS, SDSS, GAIA]:
    """Get an optical source to form full table at home page.

    :param master_source: source to get optical object for.
    :param survey: survey name.

    :return: optical object.
    """
    if survey == "LS":
        return master_source.ls_dup

    if survey == "PS":
        return master_source.ps_dup

    if survey == "SDSS":
        return master_source.sdss_dup

    if survey == "GAIA":
        return master_source.gaia_dup

    raise ValueError(f"Invalid survey: '{survey}'")


@register.filter
def is_summary(survey):
    """add filter option in template"""
    result = 1234 if survey == 9 else survey
    return result


@register.filter
def file_exists(filepath):
    file_path = "/".join(filepath.strip("/").split('/')[1:])  # get path without images/
    full_path = os.path.join(settings.IMAGE_DATA_PATH, file_path)
    if os.path.isfile(full_path):
        return filepath
    else:
        new_filepath = 'images/file_not_found.pdf' if filepath[-3:] == 'pdf' else 'images/file_not_found.png'
        return new_filepath


@register.filter
def sec_in_deg(sec):
    """add filter option in template"""
    result = sec / 3600
    return result


@register.simple_tag
def relative_url(value, field_name, urlencode=None):
    url = '?{}={}'.format(field_name, value)
    if urlencode:
        querystring = urlencode.split('&')
        filtered_querystring = filter(lambda p: p.split('=')[0] != field_name, querystring)
        encoded_querystring = '&'.join(filtered_querystring)
        url = '{}&{}'.format(url, encoded_querystring)
    return url


@register.filter
def get_opt_survey(opt_survey_sources):
    """get not None opt survey looking at dict"""
    opt_sources = opt_survey_sources.get('LS', None)
    if opt_survey_sources.get('LS', None):
        return 'LS'
    else:
        return 'PS'


@register.filter
def get_opt_sources(opt_survey_sources, survey):
    """get not None opt_sources from dict"""
    opt_sources = opt_survey_sources.get(survey, None)
    return opt_sources


@register.filter
def get_survey_color(opt_survey):
    """get color for opt survey markers"""
    if opt_survey == 'LS':
        return 'fuchsia'
    elif opt_survey == 'SDSS':
        return 'yellow'
    elif opt_survey == 'PS':
        return 'blue'
    else:
        return 'limegreen'


@register.simple_tag
def opt_row_class(opt_source, master_source, sep=None):
    if master_source in opt_source.dup_xray.all():
        return "table-success"
    elif sep and sep < 1.1*master_source.pos_r98:
        return "table-primary"
    else:
        return ""


@register.filter
def get_sep(master_source, opt_source):
    """get separation between master source and optical source"""
    c_xray = SkyCoord(ra=master_source.RA*u.degree, dec=master_source.DEC*u.degree, distance=1*u.pc, frame='icrs')
    c_opt = SkyCoord(ra=opt_source.ra*u.degree, dec=opt_source.dec*u.degree, distance=1*u.pc, frame='icrs')
    sep = c_xray.separation(c_opt)
    return sep.arcsecond


@register.filter
def is_gaia_star(master_source, opt_id):
    """get separation between master source and optical source"""
    gaia_source = master_source.gaia_sources.filter(opt_id=opt_id, star=True)
    return gaia_source.exists()


@register.filter
def field_verbose_name(obj: Model, field: str) -> str:
    """A tag to get a human-readable name for object's field.

    Some types of fields do not have verbose name. In that case ordinary name
    is returned.
    """
    model_field = obj._meta.get_field(field)
    try:
        return model_field.verbose_name
    except AttributeError:
        return model_field.name


@register.simple_tag
def show_value_for_boolean_or_unknown(
        value: bool or None,
        true: str, false: str, unknown: str = ""):

    if value is None:
        return unknown

    return true if value else false
