from django.shortcuts import render
from django.http import HttpResponse
from .models import *

def home(request):
    survey = Survey.objects.get(name=1)
    sources = Source.objects.filter(survey=survey)

    fields = Source._meta.get_fields()
    return render(request, 'home.html', {'sources': sources, 'fields': fields})
