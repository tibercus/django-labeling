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
    surveys = get_list_or_404(Survey)
    source_f = get_object_or_404(Source, pk=pk)  # get object-source chosen by user on main page
    dup_sources = Source.objects.filter(dup_id=source_f.dup_id)  # get all object-sources related to this source
    user = User.objects.first()  # TODO: get the currently logged in user
    if request.method == 'POST':
        source_id = request.POST.get('source_id', None)  # get from hidden field source pk for current tab
        source = dup_sources.get(pk=source_id)  # get current object-source
        # check if source already has gen_comment to use it as instance later
        if source.gen_comment:
            comment = source.gen_comment
        else:
            comment = Comment.objects.create(edit_by=user)

        form = NewCommentForm(request.POST, instance=comment)  # use existing or created comment

        if form.is_valid():
            form.save()

            source.comment = comment.comment
            source.source_class = comment.source_class
            if not source.gen_comment:
                source.gen_comment = comment
            source.save()

            return redirect('source', pk=source_id)
    else:
        form = NewCommentForm()
    return render(request, 'source.html', {'surveys': surveys, 'source_f': source_f, 'dup_sources': dup_sources, 'form': form})