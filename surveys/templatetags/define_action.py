from django import template
register = template.Library()

@register.simple_tag
def define(val=None):
    """define variable with value"""
    return val

@register.filter
def to_str(value):
    """converts int to string"""
    return str(value)