from django.shortcuts import render, get_object_or_404, get_list_or_404
from django.http import HttpResponse
from .models import *

def home(request):
    surveys = get_list_or_404(Survey)
    # sources = Source.objects.all()

    fields = Source._meta.get_fields()
    return render(request, 'home.html', {'surveys': surveys, 'fields': fields})

def source(request, pk):
    source = get_object_or_404(Source, pk=pk)
    return render(request, 'source.html', {'source': source })