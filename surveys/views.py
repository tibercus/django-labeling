from collections import defaultdict

from django.contrib.auth.models import User
from django.shortcuts import render, redirect, get_object_or_404, get_list_or_404
from .forms import NewCommentForm, OptCommentForm, OptCounterpartForm
from django.http import HttpResponse
from django.contrib.auth.decorators import login_required
from datetime import datetime
from .models import *

from django.utils.timezone import make_aware
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

from django.db.models import F, Count

from surveys.utils import cone_search_filter, change_opt_cp

import csv


@login_required
def home(request):
    """A view for home page. Renders list of sources."""
    master_fields = ['RA', 'DEC', 'GLON', 'GLAT', 'EXT', 'R98', 'LIKE']
    sort_fields = ['RA', 'DEC', 'GLON', 'GLAT', 'LIKE', 'RATIO_e2e1', 'RATIO_e3e2', 'RATIO_e4e3']
    # form field names of pre-class filters
    pre_class_filters = ['GAIA EDR3 Star', 'AGN Wise', 'TDE v.3', 'Final class', 'Preliminary class']
    # filter meta objects
    f = MetaObjFilter(request.GET, queryset=MetaObject.objects.all().order_by(F('pk').desc(nulls_last=True)), user=request.user)
    meta_queryset = f.qs
    # print current GET request
    # print(f'Request: {request.GET.urlencode}')
    # print(f'Sort by: {request.GET.get("sort_by")}')

    sort_by_group = request.GET.get("sort_by_group")
    if sort_by_group:
        meta_queryset = meta_queryset.annotate(num_obj=Count('meta_group__meta_objects')).filter(num_obj=1)

    field_order_by = request.GET.get("sort_by")
    if field_order_by:
        meta_queryset = meta_queryset.order_by(F(field_order_by).desc(nulls_last=True))

    # make cone search with requested RA, DEC, r
    cone_search = None
    cs_ra = request.GET.get("cs_RA"); cs_dec = request.GET.get("cs_DEC"); cs_r = request.GET.get("cs_r")
    if cs_ra and cs_dec and cs_r:
        cone_search = 'Cone Search for \nRA: {0} \nDEC: {1} \nR: {2}'.format(cs_ra, cs_dec, cs_r)
        # print(cone_search)
        meta_queryset = cone_search_filter(meta_queryset, cs_ra, cs_dec, cs_r)

    # show total count for query
    meta_count = meta_queryset.count()
    # which page to show
    page = request.GET.get('page', 1)
    # print(f'Previous req:', QueryDict(request.META.get('HTTP_REFERER')[23:]))
    # 25 meta_objects per page
    paginator = Paginator(meta_queryset, 25)

    try:
        meta_objects = paginator.page(page)
    except PageNotAnInteger:
        # fallback to the first page
        meta_objects = paginator.page(1)
    except EmptyPage:
        # probably the user tried to add a page number
        # in the url, so we fallback to the last page
        meta_objects = paginator.page(paginator.num_pages)

    bookmarks = MetaObjFilterBookmark.objects.all()

    return render(
        request,
        'home.html',
        {
            'filter': f,
            'meta_objects': meta_objects,
            'meta_fields': MetaObject.fields_to_show(),
            'master_fields': master_fields,
            'sort_fields': sort_fields,
            'pre_class_filters': pre_class_filters,
            'meta_count': meta_count,
            'cone_search': cone_search,
            'bookmarks': bookmarks,
        }
    )


@login_required
def source(request, pk):
    surveys = get_list_or_404(Survey)
    meta_object = get_object_or_404(MetaObject, pk=pk)  # get meta object chosen by user on main page
    meta_group = meta_object.meta_group
    sources = meta_object.object_sources.all()
    # get master source for meta object
    master_source = sources.get(survey__name=meta_object.master_survey)
    # print(f'Master source:{master_source}\n')

    opt_surveys = ['LS', 'PS', 'SDSS', 'GAIA']
    # get list of query sets - optical sources from different surveys
    opt_sources = []
    opt_sources_ids = []
    for s_name in opt_surveys:
        s_name_sources = eROSITA.get_opt_survey_sources(master_source, s_name)
        opt_sources.append(s_name_sources)
        # get opt_ids from all opt sources
        if s_name_sources:
            opt_sources_ids.append(list(s_name_sources.values_list('opt_id', flat=True)))

    # list of lists to list
    opt_sources_ids = sum(opt_sources_ids, [])
    opt_sources_ids = list(set(opt_sources_ids))
    print('All opt ids:', opt_sources_ids)

    # zip surveys and corresponding optical sources for template nested nav-tabs
    opt_survey_sources = dict(zip(opt_surveys, opt_sources))
    # print(f'Opt Sources by Survey: {opt_survey_sources}\n')

    # get class chosen by superuser
    try:
        admin_comment = meta_object.comments.get(created_by__is_superuser=True)
        admin_class = admin_comment.source_class
    except Comment.DoesNotExist:
        admin_class = None
    except Comment.MultipleObjectsReturned:
        admin_class = defaultdict(list)
        for comment in meta_object.comments.filter(
                created_by__is_superuser=True):
            admin_class[comment.source_class].append(
                comment.created_by.username
            )

        admin_class = ", ".join(
            f"{source_class} (by {', '.join(users)})"
            for source_class, users in admin_class.items()
        )

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
                # make requested source - master
                MetaObject.find_master_source(meta_object, req_source)
                # calculate TDE v.3 flags
                MetaObject.calculate_tde_v3(meta_object)
                MetaObject.calculate_tde_v3_ls(meta_object)

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

        # Change optical counterpart
        elif 'opt_counterpart' in request.POST:
            opt_cp_form = OptCounterpartForm(opt_sources_ids=opt_sources_ids, data=request.POST)
            # get name of optical survey
            survey_name = request.POST.get('opt_survey')
            if opt_cp_form.is_valid():
                print('Opt survey: ', survey_name)
                print('New counterpart id:', opt_cp_form.cleaned_data['cp_id'])
                print(change_opt_cp(master_source, survey_name, int(opt_cp_form.cleaned_data['cp_id'])))

            return redirect('source', pk=meta_object.pk)

    else:
        # create form for xray source
        form = NewCommentForm()
        # create form for optical source
        opt_form = OptCommentForm()
        # opt counterpart
        opt_cp_form = OptCounterpartForm(opt_sources_ids)

    return render(request, 'source.html', {'surveys': surveys, 'meta_group': meta_group, 'meta_object': meta_object,
                                           'sources': sources, 'admin_class': admin_class, 'form': form,
                                           'opt_form': opt_form, 'opt_cp_form': opt_cp_form, 'opt_surveys': opt_surveys,
                                           'master_source': master_source, 'opt_survey_sources': opt_survey_sources})


@login_required
def criteria(request):
    # show page with pre-class criteria and it's description
    criteria_list = [
        (
            'TDE v.1',
            '',
            'ID_e1 == -1 & ID_e2 == -1 & ID_e3 == -1 & ID_e123 == -1 '
                '& flag_xray==0  & flag_radio==0 & g_s != 1 '
                '&  (qual != 0 & qual != 2) & flag_agn_wise != 1 '
                '& RATIO_e4e3 > 10',
            'Отсутствие любого рентгеновского детектирования в прошлом и'
                'детектирований в радио, не звзда и не АЯГ',
        ),
        (
            'TDE v.2',
            '',
            'ID_e1 == -1 & ID_e2 == -1 & ID_e3 == -1 & ID_e123 == -1 '
                '& g_s != 1 &  (qual != 0 & qual != 2) & flag_agn_wise != 1 '
                '& RATIO_e4e3 > 7',
            'Текущая версия',
        ),
        (
            'TDE v.3',
            '',
            'ID_e3 == -1 & g_s != 1 &  (qual != 0 & qual != 2) '
                '& flag_agn_wise != 1 & RATIO_e4e3 > 7',
            'Предложенная, но не опробованная',
        ),
        (
            'GAIA Star',
            '',
            'parallax_over_error > 5 or pmra/pmra_error > 5 '
                'or pmdec / pmdec_error > 5',
            'Звезда по данным Гайя (g_s) eDR3',
        ),
        (
            'AGN WISE',
            '',
            'w1-w2 > 0.8',
            'АГН по цветам WISE (flag_agn_wise); If ls_flux_w1 < 0 '
                'or ls_flux_w2 < 0 then flag_agn_wise for this '
                'LS source = False',
        ),
    ]

    for sources_filter in MetaObjFilterBookmark.objects.all().order_by("name"):
        criteria_list.append(
            (
                sources_filter.name,
                sources_filter.author.first_name or sources_filter.author,
                sources_filter.human_readable_criteria,
                sources_filter.description,
            )
        )

    return render(request, 'criteria.html', {'criteria_list': criteria_list})


@login_required
def export_meta_csv(request):
    # load Meta Objects in csv table
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="meta_objects.csv"'
    meta_fields = MetaObject.fields_to_show()

    writer = csv.writer(response)
    writer.writerow(meta_fields)

    meta_objects = MetaObject.objects.all().values_list(*meta_fields)
    for meta_obj in meta_objects:
        writer.writerow(meta_obj)

    return response
