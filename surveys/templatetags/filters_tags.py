from django import template
from ..models import *
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
    source = sources.get(survey=survey)
    return source