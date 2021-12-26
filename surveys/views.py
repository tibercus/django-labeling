from django.contrib.auth.models import User
from django.shortcuts import render, redirect, get_object_or_404, get_list_or_404
from .forms import NewCommentForm, OptCommentForm
from django.http import HttpResponse
from django.contrib.auth.decorators import login_required
from datetime import datetime
from .models import *


@login_required
def home(request):
    # surveys = get_list_or_404(Survey)
    surveys = Survey.objects.all()
    return render(request, 'home.html', {'surveys': surveys, 'fields': Survey.get_fields_to_show()})


@login_required
def source(request, pk):
    # for parsing source fields on source page
    postfixes = ['_e1', '_e2', '_e3', '_e4', '_e5', '_e6', '_e7', '_e8']
    base_fields = [field.name for field in Source._meta.get_fields() if not field.name[-3:] in postfixes]

    opt_surveys = ['Legacy Survey', 'SDSS', 'Pan-STARRS', 'GAIA', 'WISE']

    surveys = get_list_or_404(Survey)
    prim_source = get_object_or_404(Source, pk=pk)  # get object-source chosen by user on main page
    dup_sources = None  # to prevent using dup_id == None
    if prim_source.dup_id:
        dup_sources = Source.objects.filter(dup_id=prim_source.dup_id)  # get all object-sources related to this source
    if request.method == 'POST':
        # Make master source-object or make normal
        if 'master' in request.POST and request.user.is_superuser:
            prim_source.master_source = False if prim_source.master_source else True
            prim_source.save()

            return redirect('source', pk=prim_source.pk)

        # Make superuser comment final for xray_source by superuser
        elif 'final' in request.POST and request.user.is_superuser:
            comment = prim_source.comments.get(created_by=request.user)
            prim_source.comment = comment.comment
            prim_source.source_class = comment.source_class
            prim_source.save()

            return redirect('source', pk=prim_source.pk)

        # Create/Edit comment for XRAY SOURCE
        elif 'x_comment' in request.POST:
            # Edit existing comment or create new one
            try:
                comment = Comment.objects.get(source=prim_source, created_by=request.user)
                comment.updated_at = datetime.now()
            except Comment.DoesNotExist:
                comment = Comment.objects.create(comment='create', source=prim_source, created_by=request.user)

            form = NewCommentForm(request.POST, instance=comment)  # use existing or created comment
            if form.is_valid():
                form.save()
                comment.save()

            return redirect('source', pk=prim_source.pk)

        # Create/Edit comment for OPTICAL OBJECT
        elif 'opt_comment' in request.POST:
            opt_id = request.POST.get('opt_source_id')
            opt_source = get_object_or_404(OptSource, opt_id=opt_id, name=prim_source.name)
            # Edit existing comment or create new one
            try:
                opt_comment = OptComment.objects.get(opt_source=opt_source, created_by=request.user)
                opt_comment.updated_at = datetime.now()
            except OptComment.DoesNotExist:
                opt_comment = OptComment.objects.create(comment='create', opt_source=opt_source,
                                                        created_by=request.user)

            opt_form = OptCommentForm(request.POST, instance=opt_comment)
            if opt_form.is_valid():
                opt_form.save()
                opt_comment.save()

            return redirect('source', pk=prim_source.pk)

    else:
        form = NewCommentForm()
        opt_form = OptCommentForm()

    return render(request, 'source.html', {'surveys': surveys, 'source': prim_source, 'base_fields': base_fields,
                                           'dup_sources': dup_sources, 'form': form,
                                           'opt_form': opt_form, 'opt_surveys': opt_surveys})
