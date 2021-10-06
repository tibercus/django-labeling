from django.shortcuts import render
from django.http import HttpResponse
from .models import *

def home(request):
    surveys = Survey.objects.all()
    # sources = Source.objects.all()

    fields = Source._meta.get_fields()
    return render(request, 'home.html', {'surveys': surveys, 'fields': fields})
