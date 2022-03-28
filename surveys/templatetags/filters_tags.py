from django import template
from django.core.files.storage import default_storage
from ..models import *

from django.conf import settings
import os

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
def get_master_source(meta_object):
    """add filter option in template"""
    try:
        sources = meta_object.object_sources.all()
        master_source = sources.get(survey__name=meta_object.master_survey)
    except eROSITA.DoesNotExist:
        master_source = None
    return master_source


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
def get_survey_color(opt_survey):
    """get color for opt survey markers"""
    if opt_survey == 'LS':
        return 'fuchsia'
    elif opt_survey == 'SDSS':
        return 'yellow'
    else:
        return 'blue'


@register.simple_tag
def opt_row_class(opt_source, master_source):
    if master_source in opt_source.dup_xray.all():
        return "table-primary"
    else:
        return ""
