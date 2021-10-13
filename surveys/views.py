from django.contrib.auth.models import User
from django.shortcuts import render, redirect, get_object_or_404, get_list_or_404
from .forms import NewCommentForm
from django.http import HttpResponse
from .models import *

def home(request):
    surveys = get_list_or_404(Survey)
    # sources = Source.objects.all()

    fields = [field.name for field in Source._meta.get_fields()]
    return render(request, 'home.html', {'surveys': surveys, 'fields': fields})

def source(request, pk):
    source = get_object_or_404(Source, pk=pk)
    user = User.objects.first()  # TODO: get the currently logged in user
    if request.method == 'POST':
        form = NewCommentForm(request.POST)
        if form.is_valid():
            comment = form.save(commit=False)
            comment.edit_by = user
            comment.save()

            source.gen_comment = comment
            source.comment = comment.comment
            source.source_class = comment.source_class
            source.save()
            return redirect('source', pk=source.pk)  # TODO: redirect to the created topic page
    else:
        form = NewCommentForm()
    return render(request, 'source.html', {'source': source, 'form': form})