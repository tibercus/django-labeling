from django.contrib.auth.models import User
from django.shortcuts import render, redirect, get_object_or_404, get_list_or_404
from .forms import NewCommentForm
from django.http import HttpResponse
from django.contrib.auth.decorators import login_required
from datetime import datetime
from .models import *

def home(request):
    # surveys = get_list_or_404(Survey)
    surveys = Survey.objects.all()
    # TODO: Think about summary survey
    f_list = ['comments', 'master_source', 'survey', 'opt_sources']
    fields = [field.name for field in Source._meta.get_fields() if not field.name in f_list]
    return render(request, 'home.html', {'surveys': surveys, 'fields': fields})

@login_required
def source(request, pk):
    # for parsing source fields on source page
    postfixes = ['_e1', '_e2', '_e3', '_e4', '_e5', '_e6', '_e7', '_e8']
    base_fields = [field.name for field in Source._meta.get_fields() if not field.name[-3:] in postfixes]

    surveys = get_list_or_404(Survey)
    prim_source = get_object_or_404(Source, pk=pk)  # get object-source chosen by user on main page
    dup_sources = Source.objects.filter(dup_id=prim_source.dup_id)  # get all object-sources related to this source
    if request.method == 'POST':
        # source_id = request.POST.get('source_id', None)  # get from hidden field source pk for current tab
        # source = dup_sources.get(pk=source_id)  # get current object-source
        try:
            comment = Comment.objects.get(source=prim_source, created_by=request.user)
            comment.updated_at = datetime.now()
        except Comment.DoesNotExist:
            comment = Comment.objects.create(comment='create', source=prim_source, created_by=request.user)

        # Make master source-object or make normal
        if request.POST.get('master', False) and request.user.is_superuser:
            # to prevent from getting several master sources
            # if not source.master_source:
            #     for src in list(dup_sources.filter(master_source=True)):
            #         src.master_source = False
            #         src.save()

            prim_source.master_source = False if prim_source.master_source else True
            prim_source.save()

            return redirect('source', pk=prim_source.pk)

        # Create/Edit comment
        else:
            form = NewCommentForm(request.POST, instance=comment)  # use existing or created comment

            if form.is_valid():
                form.save()
                comment.save()

                if request.user.is_superuser:
                    prim_source.comment = form.cleaned_data.get('comment')
                    prim_source.source_class = comment.source_class
                    prim_source.save()

            return redirect('source', pk=prim_source.pk)
    else:
        form = NewCommentForm()

    return render(request, 'source.html', {'surveys': surveys, 'source': prim_source, 'base_fields': base_fields,
                                           'dup_sources': dup_sources, 'form': form})