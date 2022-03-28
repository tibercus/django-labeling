from django.contrib.auth.models import User
from django.shortcuts import render, redirect, get_object_or_404, get_list_or_404
from .forms import NewCommentForm, OptCommentForm
from django.http import HttpResponse
from django.contrib.auth.decorators import login_required
from datetime import datetime
from .models import *

from django.utils.timezone import make_aware
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

from django.db.models import F, Count
from django.http import QueryDict


@login_required
def home(request):
    master_fields = ['RA', 'DEC', 'GLON', 'GLAT', 'EXT', 'R98', 'LIKE']
    sort_fields = ['RA', 'DEC', 'GLON', 'GLAT', 'LIKE', 'RATIO_e2e1', 'RATIO_e3e2', 'RATIO_e4e3']
    # filter meta objects
    f = MetaObjFilter(request.GET, queryset=MetaObject.objects.all().order_by(F('pk').desc(nulls_last=True)))
    meta_queryset = f.qs
    # print current GET request and previous
    print(f'Request: {request.GET.urlencode}')
    print(f'Sort by: {request.GET.get("sort_by")}')
    print(f'Sort by group: {request.GET.get("sort_by_group")}')

    sort_by_group = request.GET.get("sort_by_group")
    if sort_by_group:
        meta_queryset = meta_queryset.annotate(num_obj=Count('meta_group__meta_objects')).filter(num_obj=1)

    field_order_by = request.GET.get("sort_by")
    if field_order_by:
        meta_queryset = meta_queryset.order_by(F(field_order_by).desc(nulls_last=True))

    # which page to show
    page = request.GET.get('page', 1)
    # print(f'Previous req:', QueryDict(request.META.get('HTTP_REFERER')[23:]))
    # 50 meta_objects per page
    paginator = Paginator(meta_queryset, 10)

    try:
        meta_objects = paginator.page(page)
    except PageNotAnInteger:
        # fallback to the first page
        meta_objects = paginator.page(1)
    except EmptyPage:
        # probably the user tried to add a page number
        # in the url, so we fallback to the last page
        meta_objects = paginator.page(paginator.num_pages)

    return render(request, 'home.html', {'filter': f, 'meta_objects': meta_objects, 'meta_fields': MetaObject.fields_to_show(),
                                         'master_fields': master_fields, 'sort_fields': sort_fields})


@login_required
def source(request, pk):
    surveys = get_list_or_404(Survey)
    meta_object = get_object_or_404(MetaObject, pk=pk)  # get meta object chosen by user on main page
    meta_group = meta_object.meta_group
    sources = meta_object.object_sources.all()
    # get master source for meta object
    master_source = sources.get(survey__name=meta_object.master_survey)
    print(f'Master source:{master_source}\n')

    opt_surveys = ['LS', 'PS', 'SDSS']
    # get list of query sets - optical sources from different surveys
    opt_sources = []
    for s_name in opt_surveys:
        opt_sources.append(eROSITA.get_opt_survey_sources(master_source, s_name))

    # zip surveys and corresponding optical sources for template nested nav-tabs
    opt_survey_sources = dict(zip(opt_surveys, opt_sources))
    print(f'Opt Sources by Survey: {opt_survey_sources}\n')

    # make flat array from list of query sets
    # opt_sources_flat = sum([list(opt_s) for opt_s in opt_sources if opt_s], [])
    # print(f'Flat array: {opt_sources_flat}')

    # get class chosen by superuser
    try:
        admin_comment = meta_object.comments.get(created_by__is_superuser=True)
        admin_class = admin_comment.source_class
    except Comment.DoesNotExist:
        admin_class = None

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
                # TODO: test this part
                meta_object.RA = req_source.RA
                meta_object.DEC = req_source.DEC
                meta_object.GLON = req_source.GLON
                meta_object.GLAT = req_source.GLAT
                meta_object.EXT = req_source.EXT
                meta_object.R98 = req_source.pos_r98
                meta_object.LIKE = req_source.DET_LIKE_0
                meta_object.save()

            return redirect('source', pk=meta_object.pk)

        # Make superuser comment final for meta object by superuser
        elif 'final' in request.POST and request.user.is_superuser:
            comment = meta_object.comments.get(created_by=request.user)
            meta_object.comment = comment.comment
            meta_object.object_class = comment.source_class
            meta_object.save()

            return redirect('source', pk=meta_object.pk)

        # Create/Edit comment for xray data
        elif 'x_comment' in request.POST:
            # Edit existing comment or create new one
            try:
                comment = Comment.objects.get(meta_source=meta_object, created_by=request.user)
                comment.updated_at = make_aware(datetime.now())
            except Comment.DoesNotExist:
                comment = Comment.objects.create(comment='create', meta_source=meta_object, created_by=request.user)

            form = NewCommentForm(request.POST, instance=comment)  # use existing or created comment
            if form.is_valid():
                form.save()
                comment.save()

            return redirect('source', pk=meta_object.pk)

        # Create/Edit comment for xray data
        elif 'opt_comment' in request.POST:
            # Edit existing opt_comment or create new one
            try:
                opt_comment = OptComment.objects.get(meta_source=meta_object, created_by=request.user)
                opt_comment.updated_at = make_aware(datetime.now())
            except OptComment.DoesNotExist:
                opt_comment = OptComment.objects.create(comment='create', meta_source=meta_object, created_by=request.user)

            opt_form = OptCommentForm(request.POST, instance=opt_comment)  # use existing or created optical comment
            if opt_form.is_valid():
                opt_form.save()
                opt_comment.save()

            return redirect('source', pk=meta_object.pk)

    else:
        # create form for xray source
        form = NewCommentForm()
        # create form for optical source
        opt_form = OptCommentForm()

    return render(request, 'source.html', {'surveys': surveys, 'meta_group': meta_group, 'meta_object': meta_object,
                                           'sources': sources, 'admin_class': admin_class, 'form': form,
                                           'opt_form': opt_form, 'opt_surveys': opt_surveys,
                                           'master_source': master_source, 'opt_survey_sources': opt_survey_sources})


@login_required
def criteria(request):
    criteria_list = [('TDE', 'Criteria 1: Parameter A > a1, Parameter B > b2', 'Description: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'),
                     ('TDE', 'Criteria 2: Parameter B > b1, Parameter C > c2', 'Description: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'),
                     ('TDE', 'Criteria 3: Parameter D > d1, Parameter E > e2', 'Description: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'),
                     ('NOT TDE', 'Criteria 1: Parameter A < a1, Parameter B < b2', 'Description: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'),
                     ('NOT TDE', 'Criteria 2: Parameter B < b1, Parameter C < c2', 'Description: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'),
                     ('NOT TDE', 'Criteria 3: Parameter D < d1, Parameter E < e2', 'Description: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.')]
    return render(request, 'criteria.html', {'criteria_list': criteria_list})
