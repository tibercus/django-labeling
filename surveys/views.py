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
    meta_objects = MetaObject.objects.all()
    return render(request, 'home.html', {'meta_objects': meta_objects, 'meta_fields': MetaObject.fields_to_show()})


@login_required
def source(request, pk):
    # for parsing source fields on source page
    postfixes = ['_e1', '_e2', '_e3', '_e4', '_e5', '_e6', '_e7', '_e8']
    base_fields = [field.name for field in MetaObject._meta.get_fields() if not field.name[-3:] in postfixes]

    surveys = get_list_or_404(Survey)
    meta_object = get_object_or_404(MetaObject, pk=pk)  # get meta object chosen by user on main page
    meta_group = meta_object.meta_group
    sources = meta_object.object_sources.all()
    # TODO: refactor POST request
    if request.method == 'POST':
        # Make source master/not master by superuser
        if 'master' in request.POST and request.user.is_superuser:
            # get id of requested source
            source_id = request.POST.get('source_id')
            req_source = sources.get(pk=source_id)
            if req_source.survey.name == meta_object.master_survey:
                # make master source - source with max DET_LIKE_0
                MetaObject.find_master_source(meta_object)
            else:
                meta_object.master_name = req_source.name
                meta_object.master_survey = req_source.survey.name
                # TODO: uncomment later
                # meta_object.RA = req_source.RA
                # meta_object.DEC = req_source.DEC
                # meta_object.EXT = req_source.EXT
                # meta_object.R98 = req_source.pos_r98
                # meta_object.LIKE = req_source.DET_LIKE_0
                meta_object.save()

            return redirect('source', pk=meta_object.pk)

        # Make superuser comment final for xray_source by superuser
        elif 'final' in request.POST and request.user.is_superuser:
            comment = meta_object.comments.get(created_by=request.user)
            meta_object.comment = comment.comment
            meta_object.object_class = comment.source_class
            meta_object.save()

            return redirect('source', pk=meta_object.pk)

        # Create/Edit comment for XRAY SOURCE
        elif 'x_comment' in request.POST:
            # Edit existing comment or create new one
            try:
                comment = Comment.objects.get(meta_source=meta_object, created_by=request.user)
                comment.updated_at = datetime.now()
            except Comment.DoesNotExist:
                comment = Comment.objects.create(comment='create', meta_source=meta_object, created_by=request.user)

            form = NewCommentForm(request.POST, instance=comment)  # use existing or created comment
            if form.is_valid():
                form.save()
                comment.save()

            return redirect('source', pk=meta_object.pk)

    else:
        # create form for xray source
        form = NewCommentForm()
        # create form for optical source
        # opt_form = OptCommentForm()

    return render(request, 'source.html', {'surveys': surveys, 'meta_group': meta_group, 'meta_object': meta_object,
                                           'base_fields': base_fields, 'sources': sources, 'form': form})
