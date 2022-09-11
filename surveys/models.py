"""Module with Django models.

Contains models for comments, X-ray sources in eRosita survey and
sources in required optical surveys.

TODO Refactor models: create several smaller modules.
TODO Refactor models: add verbose names for attributes."""

from typing import Tuple, List

import re
from typing import Type

from django.db import models
from django.utils.text import Truncator
from django.contrib.auth.models import User
from itertools import zip_longest
from django.db.models import Max

import django_filters
from django_filters.widgets import BooleanWidget
from django import forms
from django.db.models import FloatField, ExpressionWrapper, F, Func, Case, When, Value, IntegerField
from decimal import Decimal

import astropy.units as u
from astropy.coordinates import SkyCoord

from surveys.utils import string_representation


# Class for files from where sources were loaded
class OriginFile(models.Model):
    file_name = models.CharField(max_length=200)
    version = models.PositiveIntegerField(default=1, blank=True, null=True)
    description = models.TextField(max_length=500, blank=True, null=True)  # Maybe dont need it

    def __str__(self):
        return 'MetaSource: {}'.format(self.file_name)


# Class for Meta Objects with common eROSITA sources
class MetaGroup(models.Model):
    # Pavel id in master table
    meta_ind = models.PositiveIntegerField()
    master_name = models.CharField(max_length=200, blank=True, null=True)
    master_survey = models.PositiveIntegerField(blank=True, null=True)
    # number of related sources of primary meta object
    max_sources_num = models.PositiveIntegerField(blank=True, null=True)

    def __str__(self):
        return '{} - MetaGroup: {}'.format(self.meta_ind, self.master_name)


# Class for objects of master table
class MetaObject(models.Model):
    # Pavel id in master table
    meta_ind = models.PositiveIntegerField(blank=True, null=True)
    master_name = models.CharField(max_length=200, blank=True, null=True)
    master_survey = models.PositiveIntegerField(blank=True, null=True,
                                                verbose_name="Master survey")
    RA = models.FloatField(verbose_name="Ra")
    DEC = models.FloatField(verbose_name="Dec")
    # add galactic coordinates
    GLON = models.FloatField(blank=True, null=True)
    GLAT = models.FloatField(blank=True, null=True)

    unchange_flag = models.BooleanField(default=False, blank=True, null=True)

    comment = models.TextField(max_length=2000, blank=True, null=True)

    CLASS_CHOICES = [
        ('TDE', 'Class TDE'),
        ('AGN', 'Class AGN'),
        ('Galactic', 'Class Galactic'),
        ('Other', 'Other Class'),
        (None, 'Unknown'),
    ]

    object_class = models.CharField(
        max_length=100,
        choices=CLASS_CHOICES,
        default=None,
        blank=True, null=True,
        verbose_name="Object class",
    )

    # GAIA EDR3 Star flag of master source
    g_s = models.IntegerField(blank=True, null=True)  # values: -1, 0, 1, 2
    # GAIA EDR2 Star flag of master source
    ls_g_s = models.IntegerField(blank=True, null=True)  # values: -1, 0, 1, 2
    # AGN Wise flag of master source
    flag_agn_wise = models.BooleanField(blank=True, null=True)
    # TDE v.3 flag
    tde_v3 = models.BooleanField(blank=True, null=True)
    # LS TDE v.3 flag
    tde_v3_ls = models.BooleanField(blank=True, null=True)

    # Columns of Master Table
    EXT = models.FloatField(blank=True, null=True, verbose_name="Extension")
    R98 = models.FloatField(blank=True, null=True, verbose_name="R98")
    LIKE = models.FloatField(blank=True, null=True)

    # TODO: think about next surveys: 5,6,7,8
    D2D_e1m = models.FloatField(blank=True, null=True)
    D2D_e2m = models.FloatField(blank=True, null=True)
    D2D_e3m = models.FloatField(blank=True, null=True)
    D2D_e4m = models.FloatField(blank=True, null=True)
    D2D_e5m = models.FloatField(blank=True, null=True)
    D2D_me1 = models.FloatField(blank=True, null=True)
    D2D_me2 = models.FloatField(blank=True, null=True)
    D2D_me3 = models.FloatField(blank=True, null=True)
    D2D_me4 = models.FloatField(blank=True, null=True)
    D2D_me5 = models.FloatField(blank=True, null=True)

    EXP_e1 = models.FloatField(blank=True, null=True)
    EXP_e2 = models.FloatField(blank=True, null=True)
    EXP_e3 = models.FloatField(blank=True, null=True)
    EXP_e4 = models.FloatField(blank=True, null=True)
    EXP_e5 = models.FloatField(blank=True, null=True)
    EXP_e1234 = models.FloatField(blank=True, null=True)

    # TODO: do we need these fields???
    ID_FLAG_e1m = models.IntegerField(blank=True, null=True)
    ID_FLAG_e2m = models.IntegerField(blank=True, null=True)
    ID_FLAG_e3m = models.IntegerField(blank=True, null=True)
    ID_FLAG_e4m = models.IntegerField(blank=True, null=True)
    ID_FLAG_e5m = models.IntegerField(blank=True, null=True)
    ID_FLAG_me1 = models.IntegerField(blank=True, null=True)
    ID_FLAG_me2 = models.IntegerField(blank=True, null=True)
    ID_FLAG_me3 = models.IntegerField(blank=True, null=True)
    ID_FLAG_me4 = models.IntegerField(blank=True, null=True)
    ID_FLAG_me5 = models.IntegerField(blank=True, null=True)

    ID_e1 = models.BigIntegerField(blank=True, null=True)
    ID_e2 = models.BigIntegerField(blank=True, null=True)
    ID_e3 = models.BigIntegerField(blank=True, null=True)
    ID_e4 = models.BigIntegerField(blank=True, null=True)
    ID_e5 = models.BigIntegerField(blank=True, null=True)
    ID_e1234 = models.BigIntegerField(blank=True, null=True)

    RATIO_e2e1 = models.FloatField(blank=True, null=True)
    RATIO_e3e2 = models.FloatField(blank=True, null=True)
    RATIO_e4e3 = models.FloatField(blank=True, null=True)
    RATIO_e5e4 = models.FloatField(blank=True, null=True)

    RFLAG_e2e1 = models.IntegerField(blank=True, null=True)
    RFLAG_e3e2 = models.IntegerField(blank=True, null=True)
    RFLAG_e4e3 = models.IntegerField(blank=True, null=True)
    RFLAG_e5e4 = models.IntegerField(blank=True, null=True)

    R_NSRC_e1m = models.IntegerField(blank=True, null=True)
    R_NSRC_e2m = models.IntegerField(blank=True, null=True)
    R_NSRC_e3m = models.IntegerField(blank=True, null=True)
    R_NSRC_e4m = models.IntegerField(blank=True, null=True)
    R_NSRC_e5m = models.IntegerField(blank=True, null=True)
    R_NSRC_me1 = models.IntegerField(blank=True, null=True)
    R_NSRC_me2 = models.IntegerField(blank=True, null=True)
    R_NSRC_me3 = models.IntegerField(blank=True, null=True)
    R_NSRC_me4 = models.IntegerField(blank=True, null=True)
    R_NSRC_me5 = models.IntegerField(blank=True, null=True)

    UPLIM_e1 = models.FloatField(blank=True, null=True)
    UPLIM_e2 = models.FloatField(blank=True, null=True)
    UPLIM_e3 = models.FloatField(blank=True, null=True)
    UPLIM_e4 = models.FloatField(blank=True, null=True)
    UPLIM_e5 = models.FloatField(blank=True, null=True)
    UPLIM_e1234 = models.FloatField(blank=True, null=True)

    flag = models.IntegerField(blank=True, null=True)

    # TSTART_e1,2,3,4
    TSTART_e1 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e2 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e3 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e4 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e5 = models.CharField(max_length=100, blank=True, null=True)
    # TSTOP_e1,2,3,4
    TSTOP_e1 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e2 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e3 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e4 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e5 = models.CharField(max_length=100, blank=True, null=True)

    primary_object = models.BooleanField(default=True, blank=True, null=True)
    # Meta Group object for meta objects with common sources
    meta_group = models.ForeignKey(MetaGroup, on_delete=models.CASCADE,
                                   related_name='meta_objects',
                                   blank=True, null=True,
                                   verbose_name="Meta group")

    def __str__(self):
        return '{} - MetaObject: {}'.format(self.meta_ind, self.master_name)

    @staticmethod
    def fields_to_show():
        fields = ['id', 'meta_ind', 'master_name', 'master_survey', 'RA', 'DEC', 'GLON', 'GLAT',
                  'unchange_flag', 'comment', 'object_class', 'g_s', 'ls_g_s', 'flag_agn_wise', 'EXT', 'R98', 'LIKE',
                  'D2D_e1m', 'D2D_e2m', 'D2D_e3m', 'D2D_e4m', 'D2D_e5m', 'D2D_me1', 'D2D_me2', 'D2D_me3', 'D2D_me4', 'D2D_me5',
                  'EXP_e1', 'EXP_e2', 'EXP_e3', 'EXP_e4', 'EXP_e5', 'EXP_e1234',
                  'ID_FLAG_e1m', 'ID_FLAG_e2m', 'ID_FLAG_e3m', 'ID_FLAG_e4m', 'ID_FLAG_e5m',
                  'ID_FLAG_me1', 'ID_FLAG_me2', 'ID_FLAG_me3', 'ID_FLAG_me4', 'ID_FLAG_me5',
                  'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4', 'ID_e5', 'ID_e1234',
                  'RATIO_e2e1', 'RATIO_e3e2', 'RATIO_e4e3', 'RATIO_e5e4', 'RFLAG_e2e1', 'RFLAG_e3e2', 'RFLAG_e4e3', 'RFLAG_e5e4',
                  'R_NSRC_e1m', 'R_NSRC_e2m', 'R_NSRC_e3m', 'R_NSRC_e4m', 'R_NSRC_e5m',
                  'R_NSRC_me1', 'R_NSRC_me2', 'R_NSRC_me3', 'R_NSRC_me4', 'R_NSRC_me5',
                  'UPLIM_e1', 'UPLIM_e2', 'UPLIM_e3', 'UPLIM_e4', 'UPLIM_e5', 'UPLIM_e1234', 'flag',
                  'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTART_e4', 'TSTART_e5',
                  'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3', 'TSTOP_e4', 'TSTOP_e5']
        return fields

    def find_master_source(self, req_source=None):
        if req_source is None:
            sources = self.object_sources.all()
            master_source = None
            if sources:
                # find master source
                max_dl0 = sources.aggregate(Max('DET_LIKE_0'))['DET_LIKE_0__max']
                master_source = sources.get(DET_LIKE_0=max_dl0)
                print(f'Found master_source  - {master_source.name} with DET_LIKE_0: {max_dl0}')
        else:
            # take requested master source
            master_source = req_source
            print(f'Take requested master_source  - {master_source.name} with DET_LIKE_0: {master_source.DET_LIKE_0}.')

        if master_source:
            # take name, survey, RA, DEC, EXT, R98, LIKE from master_source
            self.master_name = master_source.name
            self.master_survey = master_source.survey.name
            self.RA = master_source.RA
            self.DEC = master_source.DEC
            self.EXT = master_source.EXT
            self.R98 = master_source.pos_r98
            self.LIKE = master_source.DET_LIKE_0
            # take galactic coordinates
            self.GLON = master_source.GLON
            self.GLAT = master_source.GLAT
            # take pre class flags
            self.g_s = master_source.g_s
            self.ls_g_s = master_source.ls_g_s
            self.flag_agn_wise = master_source.flag_agn_wise
            self.save()

    def calculate_tde_v3(self):
        # ID_e3 == -1 & g_s != 1 &  (qual != 0 & qual != 2) & flag_agn_wise != 1 & RATIO_e4e3 > 7
        survey_2_flag = self.ID_e1 == -1 and (self.g_s is not None) and self.g_s != 1 \
                        and (self.flag_agn_wise is not None) and self.flag_agn_wise != 1 \
                        and (self.RATIO_e2e1 is not None) and self.RATIO_e2e1 > 7

        survey_3_flag = self.ID_e2 == -1 and (self.g_s is not None) and self.g_s != 1 \
                        and (self.flag_agn_wise is not None) and self.flag_agn_wise != 1 \
                        and (self.RATIO_e3e2 is not None) and self.RATIO_e3e2 > 7

        survey_4_flag = self.ID_e3 == -1 and (self.g_s is not None) and self.g_s != 1 \
                        and (self.flag_agn_wise is not None) and self.flag_agn_wise != 1 \
                        and (self.RATIO_e4e3 is not None) and self.RATIO_e4e3 > 7

        self.tde_v3 = survey_2_flag or survey_3_flag or survey_4_flag
        # print(f'Surveys flags: {survey_2_flag}, {survey_3_flag}, {survey_4_flag} = {self.tde_v3}\n')
        self.save()

    def calculate_tde_v3_ls(self):
        # ID_e3 == -1 & ls_g_s != 1 &  (qual != 0 & qual != 2) & flag_agn_wise != 1 & RATIO_e4e3 > 7
        survey_2_flag = self.ID_e1 == -1 and (self.ls_g_s is not None) and self.ls_g_s != 1 \
                        and (self.flag_agn_wise is not None) and self.flag_agn_wise != 1 \
                        and (self.RATIO_e2e1 is not None) and self.RATIO_e2e1 > 7

        survey_3_flag = self.ID_e2 == -1 and (self.ls_g_s is not None) and self.ls_g_s != 1 \
                        and (self.flag_agn_wise is not None) and self.flag_agn_wise != 1 \
                        and (self.RATIO_e3e2 is not None) and self.RATIO_e3e2 > 7

        survey_4_flag = self.ID_e3 == -1 and (self.ls_g_s is not None) and self.ls_g_s != 1 \
                        and (self.flag_agn_wise is not None) and self.flag_agn_wise != 1 \
                        and (self.RATIO_e4e3 is not None) and self.RATIO_e4e3 > 7

        self.tde_v3_ls = survey_2_flag or survey_3_flag or survey_4_flag
        # print(f'Surveys flags: {survey_2_flag}, {survey_3_flag}, {survey_4_flag} = {self.tde_v3_ls}\n')
        self.save()

    def __iter__(self):
        for field in MetaObject.fields_to_show():
            value = getattr(self, field, None)
            yield field, value


# Class for filtering Meta Objects by requested fields
class MetaObjFilter(django_filters.FilterSet):

    def __init__(self, *args, **kwargs):
        # get request user
        self.user = kwargs.pop('user')
        super().__init__(*args, **kwargs)

    CHOICES = ((True, 'Primary'), (False, 'Secondary'))
    GAIA_CHOICES = ((-1, 'No sources'), (0, 'Not stars'), (1, 'Stars'), (2, 'Combined'))

    # filters for ID_ei == -1
    id_e1_gt_0 = django_filters.BooleanFilter(label='Source in e1', method='id_ei_gt_zero',
                                            widget=BooleanWidget(attrs={'class': 'custom_bool'}))

    id_e2_gt_0 = django_filters.BooleanFilter(label='Source in e2', method='id_ei_gt_zero',
                                              widget=BooleanWidget(attrs={'class': 'custom_bool'}))

    id_e3_gt_0 = django_filters.BooleanFilter(label='Source in e3', method='id_ei_gt_zero',
                                              widget=BooleanWidget(attrs={'class': 'custom_bool'}))

    id_e4_gt_0 = django_filters.BooleanFilter(label='Source in e4', method='id_ei_gt_zero',
                                              widget=BooleanWidget(attrs={'class': 'custom_bool'}))

    # filters for 'Primary' and 'EXT'
    is_primary = django_filters.BooleanFilter(
        field_name='primary_object', label='Status', required=False,
        widget=forms.RadioSelect(attrs={'class': 'custom_radio form-check d-flex justify-content-between'},
                                 choices=CHOICES),
    )

    ext_gt_0 = django_filters.BooleanFilter(label='EXT > 0', method='ext_gt_zero',
                                            widget=BooleanWidget(attrs={'class': 'custom_bool'}))

    # filters for pre-class flags of master source
    agn_wise = django_filters.BooleanFilter(field_name='flag_agn_wise', label='AGN Wise',
                                            widget=BooleanWidget(attrs={'class': 'custom_bool'}))

    gaia_star = django_filters.NumberFilter(
        field_name='g_s', label='GAIA EDR3 Star', required=False,
        widget=forms.RadioSelect(attrs={'class': 'gaia_star_radio form-check'},
                                 choices=GAIA_CHOICES),
    )

    tde_v3 = django_filters.BooleanFilter(field_name='tde_v3', label='TDE v.3',
                                          widget=BooleanWidget(attrs={'class': 'custom_bool'}))
    # filters for meta object result classes
    object_class = django_filters.ChoiceFilter(field_name='object_class', label='Final class', choices=MetaObject.CLASS_CHOICES,
                                               empty_label=None, null_label=None)

    comment_class = django_filters.ChoiceFilter(method='comments_class', label='Preliminary class', choices=MetaObject.CLASS_CHOICES,
                                                empty_label=None, null_label=None)

    class Meta:
        model = MetaObject
        fields = {'master_name': ['icontains'],
                  'comment': ['icontains'],
                  'RA': ['gt', 'lt'],
                  'DEC': ['gt', 'lt'],
                  'GLON': ['gt', 'lt'],
                  'GLAT': ['gt', 'lt'],
                  'LIKE': ['gt', 'lt'],
                  'RATIO_e2e1': ['gt', 'lt'],
                  'RFLAG_e2e1': ['gt', 'lt'],
                  'RATIO_e3e2': ['gt', 'lt'],
                  'RFLAG_e3e2': ['gt', 'lt'],
                  'RATIO_e4e3': ['gt', 'lt'],
                  'RFLAG_e4e3': ['gt', 'lt'],
                  }

    @staticmethod
    def ext_gt_zero(queryset, name, value):
        if value is not None:
            queryset = queryset.annotate(
                ext_is_gt=Case(
                    When(
                        EXT__gt=0,
                        then=Value(True)
                    ),
                    default=Value(False),
                    output_field=models.BooleanField())
            ).filter(ext_is_gt=value)

        return queryset

    @staticmethod
    def id_ei_gt_zero(queryset, name, value):
        if value is not None:
            if 'e1' in name:
                queryset = queryset.annotate(
                    id_e1_is_gt=Case(
                        When(
                            ID_e1__gt=-1,
                            then=Value(True)
                        ),
                        default=Value(False),
                        output_field=models.BooleanField())
                ).filter(id_e1_is_gt=value)
            elif 'e2' in name:
                queryset = queryset.annotate(
                    id_e2_is_gt=Case(
                        When(
                            ID_e2__gt=-1,
                            then=Value(True)
                        ),
                        default=Value(False),
                        output_field=models.BooleanField())
                ).filter(id_e2_is_gt=value)
            elif 'e3' in name:
                queryset = queryset.annotate(
                    id_e3_is_gt=Case(
                        When(
                            ID_e3__gt=-1,
                            then=Value(True)
                        ),
                        default=Value(False),
                        output_field=models.BooleanField())
                ).filter(id_e3_is_gt=value)
            elif 'e4' in name:
                queryset = queryset.annotate(
                    id_e4_is_gt=Case(
                        When(
                            ID_e4__gt=-1,
                            then=Value(True)
                        ),
                        default=Value(False),
                        output_field=models.BooleanField())
                ).filter(id_e4_is_gt=value)

        return queryset

    # look for meta objects where current user posted class = value
    def comments_class(self, queryset, name, value):
        queryset = queryset.filter(comments__created_by=self.user, comments__source_class=value) | \
                   queryset.filter(comments__created_by=self.user, comments__source_class_1=value) | \
                   queryset.filter(comments__created_by=self.user, comments__source_class_2=value)

        return queryset


# Class for surveys where transients were detected
class Survey(models.Model):
    name = models.PositiveIntegerField(unique=True)
    started_at = models.DateTimeField(auto_now_add=True)
    ended_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(max_length=500, blank=True, null=True)

    def __str__(self):
        return 'Survey {}'.format(self.name)

    def get_sources_count(self):
        return eROSITA.objects.filter(survey=self).count()


# Class for xray sources from all surveys
class eROSITA(models.Model):
    # Pavel id for master table
    survey_ind = models.PositiveIntegerField(blank=True, null=True)
    name = models.CharField(max_length=150)

    RA = models.FloatField()
    DEC = models.FloatField()

    comment = models.TextField(max_length=2000, blank=True, null=True)

    source_class = models.CharField(
        max_length=100,
        choices=MetaObject.CLASS_CHOICES,
        default=None,
        blank=True, null=True,
    )

    # GAIA EDR3 Star flag
    g_s = models.IntegerField(blank=True, null=True)  # values: -1, 0, 1, 2
    # GAIA EDR2 Star flag
    ls_g_s = models.IntegerField(blank=True, null=True)  # values: -1, 0, 1, 2
    # AGN Wise flag
    flag_agn_wise = models.BooleanField(blank=True, null=True)

    GLON = models.FloatField(blank=True, null=True)
    GLAT = models.FloatField(blank=True, null=True)
    pos_r98 = models.FloatField(blank=True, null=True)
    DET_LIKE_0 = models.FloatField(blank=True, null=True)

    ML_FLUX_0 = models.FloatField(blank=True, null=True)
    ML_FLUX_ERR_0 = models.FloatField(blank=True, null=True)
    ML_CTS_0 = models.FloatField(blank=True, null=True)
    ML_CTS_ERR_0 = models.FloatField(blank=True, null=True)
    ML_EXP_1 = models.FloatField(blank=True, null=True)

    EXT = models.FloatField(blank=True, null=True)
    EXT_LIKE = models.FloatField(blank=True, null=True)
    EXT_ERR = models.FloatField(blank=True, null=True)

    hpidx = models.BigIntegerField(blank=True, null=True)
    RADEC_ERR = models.FloatField(blank=True, null=True)

    ML_BKG_0 = models.FloatField(blank=True, null=True)
    ML_RATE_0 = models.FloatField(blank=True, null=True)
    ML_RATE_ERR_0 = models.FloatField(blank=True, null=True)

    tilenum = models.IntegerField(blank=True, null=True)
    ID_SRC = models.IntegerField(blank=True, null=True)
    ID_CLUSTER = models.IntegerField(blank=True, null=True)
    DIST_NN = models.FloatField(blank=True, null=True)
    SRCDENS = models.FloatField(blank=True, null=True)
    NH = models.FloatField(blank=True, null=True)
    RA_corr = models.FloatField(blank=True, null=True)
    DEC_corr = models.FloatField(blank=True, null=True)

    astro_indx = models.IntegerField(blank=True, null=True)
    astro_nx = models.FloatField(blank=True, null=True)
    astro_mdra = models.FloatField(blank=True, null=True)
    astro_mddec = models.FloatField(blank=True, null=True)
    astro_fit_nx = models.FloatField(blank=True, null=True)
    astro_fit_sigma = models.FloatField(blank=True, null=True)
    astro_fit_ro_opt = models.FloatField(blank=True, null=True)
    astro_flag = models.IntegerField(blank=True, null=True)

    pos_sigma_2d = models.FloatField(blank=True, null=True)
    pos_r68 = models.FloatField(blank=True, null=True)
    pos_r95 = models.FloatField(blank=True, null=True)

    ELON = models.FloatField(blank=True, null=True)
    ELAT = models.FloatField(blank=True, null=True)
    flux_05_20 = models.FloatField(blank=True, null=True)

    nH_pow = models.FloatField(blank=True, null=True)
    nH_pow_l = models.FloatField(blank=True, null=True)
    nH_pow_u = models.FloatField(blank=True, null=True)
    nH_pow_st = models.IntegerField(blank=True, null=True)

    G_pow = models.FloatField(blank=True, null=True)
    G_pow_l = models.FloatField(blank=True, null=True)
    G_pow_u = models.FloatField(blank=True, null=True)
    G_pow_st = models.IntegerField(blank=True, null=True)

    norm_pow = models.FloatField(blank=True, null=True)
    norm_pow_l = models.FloatField(blank=True, null=True)
    norm_pow_u = models.FloatField(blank=True, null=True)
    norm_pow_st = models.IntegerField(blank=True, null=True)

    c_pow = models.FloatField(blank=True, null=True)
    dof_pow = models.IntegerField(blank=True, null=True)

    nH_dbb = models.FloatField(blank=True, null=True)
    nH_dbb_l = models.FloatField(blank=True, null=True)
    nH_dbb_u = models.FloatField(blank=True, null=True)
    nH_dbb_st = models.IntegerField(blank=True, null=True)

    kT_dbb = models.FloatField(blank=True, null=True)
    kT_dbb_l = models.FloatField(blank=True, null=True)
    kT_dbb_u = models.FloatField(blank=True, null=True)
    kT_dbb_st = models.IntegerField(blank=True, null=True)

    norm_dbb = models.FloatField(blank=True, null=True)
    norm_dbb_l = models.FloatField(blank=True, null=True)
    norm_dbb_u = models.FloatField(blank=True, null=True)
    norm_dbb_st = models.IntegerField(blank=True, null=True)

    c_dbb = models.FloatField(blank=True, null=True)
    dof_dbb = models.IntegerField(blank=True, null=True)

    TSTART = models.CharField(max_length=100, blank=True, null=True)
    TSTOP = models.CharField(max_length=100, blank=True, null=True)

    # survey where source was detected
    survey = models.ForeignKey(Survey, on_delete=models.CASCADE, related_name='sources')
    # File from which source was loaded to system
    origin_file = models.ForeignKey(OriginFile, on_delete=models.CASCADE, related_name='xray_sources', blank=True, null=True)
    # to find sources that are considered one space object
    meta_objects = models.ManyToManyField(MetaObject, related_name='object_sources', blank=True)

    # ls counterpart link + separation(arcseconds)
    ls_dup = models.ForeignKey('LS', on_delete=models.SET_NULL, related_name='dup_xray', blank=True, null=True)
    ls_dup_sep = models.DecimalField(max_digits=8, decimal_places=5, blank=True, null=True)
    # sdss counterpart pk + separation(arcseconds)
    sdss_dup = models.ForeignKey('SDSS', on_delete=models.SET_NULL, related_name='dup_xray', blank=True, null=True)
    sdss_dup_sep = models.DecimalField(max_digits=8, decimal_places=5, blank=True, null=True)
    # ps counterpart pk + separation(arcseconds)
    ps_dup = models.ForeignKey('PS', on_delete=models.SET_NULL, related_name='dup_xray', blank=True, null=True)
    ps_dup_sep = models.DecimalField(max_digits=8, decimal_places=5, blank=True, null=True)
    # gaia counterpart pk + separation(arcseconds)
    gaia_dup = models.ForeignKey('GAIA', on_delete=models.SET_NULL, related_name='dup_xray', blank=True, null=True)
    gaia_dup_sep = models.DecimalField(max_digits=8, decimal_places=5, blank=True, null=True)

    def __str__(self):
        return '{} - Source: {}'.format(self.survey_ind, self.name)

    def get_comment_count(self):
        return Comment.objects.filter(source=self).count()

    def get_last_comment(self):
        return Comment.objects.filter(source=self).order_by('-created_at').first()

    def __iter__(self):
        for field in self._meta.get_fields():
            value = getattr(self, field.name, "")

            try:
                name = field.verbose_name
            except AttributeError:
                name = field.name

            yield name, value

    @staticmethod
    def fields_to_show():
        fields = ['survey_ind', 'name', 'RA', 'DEC', 'comment', 'source_class', 'GLON', 'GLAT',
                    'pos_r98', 'DET_LIKE_0', 'ML_FLUX_0', 'ML_FLUX_ERR_0', 'ML_CTS_0', 'ML_CTS_ERR_0',
                    'ML_EXP_1', 'EXT', 'EXT_LIKE', 'EXT_ERR', 'hpidx', 'RADEC_ERR', 'ML_BKG_0', 'ML_RATE_0',
                    'ML_RATE_ERR_0', 'tilenum', 'ID_SRC', 'ID_CLUSTER', 'DIST_NN', 'SRCDENS', 'NH',
                    'RA_corr', 'DEC_corr', 'astro_indx', 'astro_nx', 'astro_mdra',
                    'astro_mddec', 'astro_fit_nx', 'astro_fit_sigma', 'astro_fit_ro_opt',
                    'astro_flag', 'pos_sigma_2d', 'pos_r68', 'pos_r95',
                    'ELON', 'ELAT', 'flux_05_20', 'nH_pow', 'nH_pow_l', 'nH_pow_u',
                    'nH_pow_st', 'G_pow', 'G_pow_l', 'G_pow_u', 'G_pow_st', 'norm_pow',
                    'norm_pow_l', 'norm_pow_u', 'norm_pow_st', 'c_pow', 'dof_pow', 'nH_dbb',
                    'nH_dbb_l', 'nH_dbb_u', 'nH_dbb_st', 'kT_dbb', 'kT_dbb_l', 'kT_dbb_u',
                    'kT_dbb_st', 'norm_dbb', 'norm_dbb_l', 'norm_dbb_u', 'norm_dbb_st',
                    'c_dbb', 'dof_dbb', 'TSTART', 'TSTOP', 'survey']
        return fields


    def change_dup_source(self, survey_name, new_dup, new_dup_sep):
        if survey_name == 'LS':
            self.ls_dup = new_dup
            self.ls_dup_sep = new_dup_sep

        elif survey_name == 'PS':
            self.ps_dup = new_dup
            self.ps_dup_sep = new_dup_sep

        elif survey_name == 'SDSS':
            self.sdss_dup = new_dup
            self.sdss_dup_sep = new_dup_sep

        elif survey_name == 'GAIA':
            self.gaia_dup = new_dup
            self.gaia_dup_sep = new_dup_sep

        self.save()

    def get_opt_survey_sources(self, survey_name):
        opt_sources = None
        # TODO: think about adding coords to eROSITA fields
        # find cartesian coords for xray source
        c_xray = SkyCoord(ra=self.RA * u.deg, dec=self.DEC * u.deg, distance=1 * u.pc, frame='icrs')
        c_x = c_xray.cartesian.x.value
        c_y = c_xray.cartesian.y.value
        c_z = c_xray.cartesian.z.value

        if survey_name == 'LS':
            opt_sources = self.ls_sources.all()

        elif survey_name == 'PS':
            opt_sources = self.ps_sources.all()

        elif survey_name == 'SDSS':
            opt_sources = self.sdss_sources.all()

        elif survey_name == 'GAIA':
            opt_sources = self.gaia_sources.all()

        # order optical sources by distance from xray source
        opt_sources = opt_sources.annotate(separation=((F('c_x') - c_x)**2.0 + (F('c_y') - c_y)**2.0
                                                       + (F('c_z') - c_z)**2.0)).order_by('separation')
        return opt_sources if opt_sources.exists() else None

    class Meta:
        verbose_name_plural = 'eROSITA sources'


class Comment(models.Model):
    """Comment class for Meta-object.

    Can be posted only by superuser, and overwrites previous comment.
    Contains comment, some follow up and three class fields. First class field
    is considered as final class for meta-object.

    Technical fields are time of creation/update and FK to MetaObject.

    TODO: with two different superusers it throws and exception views.py on 105
    """
    comment = models.TextField(max_length=2000)
    follow_up = models.TextField(max_length=1000, blank=True, null=True)

    source_class = models.CharField(
        max_length=100,
        choices=MetaObject.CLASS_CHOICES,
        default=None,
        blank=True, null=True,
    )

    source_class_1 = models.CharField(
        max_length=100,
        choices=MetaObject.CLASS_CHOICES,
        default=None,
        blank=True, null=True,
    )

    source_class_2 = models.CharField(
        max_length=100,
        choices=MetaObject.CLASS_CHOICES,
        default=None,
        blank=True, null=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='comments')
    updated_at = models.DateTimeField(blank=True, null=True)

    meta_source = models.ForeignKey(MetaObject, on_delete=models.CASCADE, related_name='comments', blank=True, null=True)

    def __str__(self):
        truncated_comment = Truncator(self.comment)
        return 'Comment: {}'.format(truncated_comment.chars(10))

    class Meta:
        ordering = ('-created_by__is_superuser',)


class LS(models.Model):
    opt_id = models.PositiveIntegerField(blank=True, null=True)
    objID = models.PositiveIntegerField(blank=True, null=True)
    ra = models.FloatField(verbose_name='RA')
    dec = models.FloatField(verbose_name='Dec')
    # cartesian coordinates with radius = 1pc
    c_x = models.FloatField(blank=True, null=True)
    c_y = models.FloatField(blank=True, null=True)
    c_z = models.FloatField(blank=True, null=True)
    # add heal pix index for identification
    opt_hpidx = models.BigIntegerField()
    # agn wise flag
    flag_agn_wise = models.BooleanField(blank=True, null=True)
    # gaiaedr2 star flag
    star = models.BooleanField(blank=True, null=True)

    brick_primary = models.BooleanField(blank=True, null=True)

    maskbits = models.FloatField(blank=True, null=True)
    fitbits = models.FloatField(blank=True, null=True)
    type = models.CharField(max_length=100, blank=True, null=True)
    ra_ivar = models.FloatField(blank=True, null=True)
    dec_ivar = models.FloatField(blank=True, null=True)
    bx = models.FloatField(blank=True, null=True)
    by = models.FloatField(blank=True, null=True)
    ebv = models.FloatField(blank=True, null=True)
    mjd_min = models.FloatField(blank=True, null=True)
    mjd_max = models.FloatField(blank=True, null=True)
    ref_cat = models.CharField(max_length=100, blank=True, null=True)
    ref_id = models.FloatField(blank=True, null=True)

    pmra = models.FloatField(blank=True, null=True)
    pmdec = models.FloatField(blank=True, null=True)
    parallax = models.FloatField(blank=True, null=True)
    pmra_ivar = models.FloatField(blank=True, null=True)
    pmdec_ivar = models.FloatField(blank=True, null=True)
    parallax_ivar = models.FloatField(blank=True, null=True)
    ref_epoch = models.FloatField(blank=True, null=True)

    gaia_phot_g_mean_mag = models.FloatField(blank=True, null=True)
    gaia_phot_g_mean_flux_over_error = models.FloatField(blank=True, null=True)
    gaia_phot_g_n_obs = models.FloatField(blank=True, null=True)
    gaia_phot_bp_mean_mag = models.FloatField(blank=True, null=True)
    gaia_phot_bp_mean_flux_over_error = models.FloatField(blank=True, null=True)
    gaia_phot_bp_n_obs = models.FloatField(blank=True, null=True)
    gaia_phot_rp_mean_mag = models.FloatField(blank=True, null=True)
    gaia_phot_rp_mean_flux_over_error = models.FloatField(blank=True, null=True)
    gaia_phot_rp_n_obs = models.FloatField(blank=True, null=True)

    gaia_phot_variable_flag = models.BooleanField(blank=True, null=True)

    gaia_astrometric_excess_noise = models.FloatField(blank=True, null=True)
    gaia_astrometric_excess_noise_sig = models.FloatField(blank=True, null=True)
    gaia_astrometric_n_obs_al = models.FloatField(blank=True, null=True)
    gaia_astrometric_n_good_obs_al = models.FloatField(blank=True, null=True)
    gaia_astrometric_weight_al = models.FloatField(blank=True, null=True)

    gaia_duplicated_source = models.BooleanField(blank=True, null=True)

    gaia_a_g_val = models.FloatField(blank=True, null=True)
    gaia_e_bp_min_rp_val = models.FloatField(blank=True, null=True)
    gaia_phot_bp_rp_excess_factor = models.FloatField(blank=True, null=True)
    gaia_astrometric_sigma5d_max = models.FloatField(blank=True, null=True)
    gaia_astrometric_params_solved = models.FloatField(blank=True, null=True)

    fiberflux_g = models.FloatField(blank=True, null=True)
    fiberflux_r = models.FloatField(blank=True, null=True)
    fiberflux_z = models.FloatField(blank=True, null=True)
    fibertotflux_g = models.FloatField(blank=True, null=True)
    fibertotflux_r = models.FloatField(blank=True, null=True)
    fibertotflux_z = models.FloatField(blank=True, null=True)

    mw_transmission_g = models.FloatField(blank=True, null=True)
    mw_transmission_r = models.FloatField(blank=True, null=True)
    mw_transmission_z = models.FloatField(blank=True, null=True)
    mw_transmission_w1 = models.FloatField(blank=True, null=True)
    mw_transmission_w2 = models.FloatField(blank=True, null=True)
    mw_transmission_w3 = models.FloatField(blank=True, null=True)
    mw_transmission_w4 = models.FloatField(blank=True, null=True)

    nobs_g = models.PositiveIntegerField(blank=True, null=True)
    nobs_r = models.PositiveIntegerField(blank=True, null=True)
    nobs_z = models.PositiveIntegerField(blank=True, null=True)
    nobs_w1 = models.PositiveIntegerField(blank=True, null=True)
    nobs_w2 = models.PositiveIntegerField(blank=True, null=True)
    nobs_w3 = models.PositiveIntegerField(blank=True, null=True)
    nobs_w4 = models.PositiveIntegerField(blank=True, null=True)

    rchisq_g = models.FloatField(blank=True, null=True)
    rchisq_r = models.FloatField(blank=True, null=True)
    rchisq_z = models.FloatField(blank=True, null=True)
    rchisq_w1 = models.FloatField(blank=True, null=True)
    rchisq_w2 = models.FloatField(blank=True, null=True)
    rchisq_w3 = models.FloatField(blank=True, null=True)
    rchisq_w4 = models.FloatField(blank=True, null=True)

    fracflux_g = models.FloatField(blank=True, null=True)
    racflux_r = models.FloatField(blank=True, null=True)
    fracflux_z = models.FloatField(blank=True, null=True)
    fracflux_w1 = models.FloatField(blank=True, null=True)
    fracflux_w2 = models.FloatField(blank=True, null=True)
    fracflux_w3 = models.FloatField(blank=True, null=True)
    fracflux_w4 = models.FloatField(blank=True, null=True)

    fracmasked_g = models.FloatField(blank=True, null=True)
    fracmasked_r = models.FloatField(blank=True, null=True)
    fracmasked_z = models.FloatField(blank=True, null=True)
    fracin_g = models.FloatField(blank=True, null=True)
    racin_r = models.FloatField(blank=True, null=True)
    facin_z = models.FloatField(blank=True, null=True)

    anymask_g = models.FloatField(blank=True, null=True)
    anymask_r = models.FloatField(blank=True, null=True)
    anymask_z = models.FloatField(blank=True, null=True)
    allmask_g = models.FloatField(blank=True, null=True)
    allmask_r = models.FloatField(blank=True, null=True)
    allmask_z = models.FloatField(blank=True, null=True)

    wisemask_w1 = models.FloatField(blank=True, null=True)
    wisemask_w2 = models.FloatField(blank=True, null=True)

    psfsize_g = models.FloatField(blank=True, null=True)
    psfsize_r = models.FloatField(blank=True, null=True)
    psfsize_z = models.FloatField(blank=True, null=True)
    psfdepth_g = models.FloatField(blank=True, null=True)
    psfdepth_r = models.FloatField(blank=True, null=True)
    psfdepth_z = models.FloatField(blank=True, null=True)
    galdepth_g = models.FloatField(blank=True, null=True)
    galdepth_r = models.FloatField(blank=True, null=True)
    galdepth_z = models.FloatField(blank=True, null=True)

    nea_g = models.FloatField(blank=True, null=True)
    nea_r = models.FloatField(blank=True, null=True)
    nea_z = models.FloatField(blank=True, null=True)

    lob_nea_g = models.FloatField(blank=True, null=True)
    blob_nea_r = models.FloatField(blank=True, null=True)
    blob_nea_z = models.FloatField(blank=True, null=True)

    psfdepth_w1 = models.FloatField(blank=True, null=True)
    psfdepth_w2 = models.FloatField(blank=True, null=True)
    psfdepth_w3 = models.FloatField(blank=True, null=True)
    psfdepth_w4 = models.FloatField(blank=True, null=True)

    wise_coadd_id = models.CharField(max_length=100, blank=True, null=True)

    wise_x = models.FloatField(blank=True, null=True)
    wise_y = models.FloatField(blank=True, null=True)

    sersic = models.FloatField(blank=True, null=True)
    sersic_ivar = models.FloatField(blank=True, null=True)

    shape_r = models.FloatField(blank=True, null=True)
    shape_r_ivar = models.FloatField(blank=True, null=True)
    shape_e1 = models.FloatField(blank=True, null=True)
    shape_e1_ivar = models.FloatField(blank=True, null=True)
    shape_e2 = models.FloatField(blank=True, null=True)
    shape_e2_ivar = models.FloatField(blank=True, null=True)

    healpix_id_log2nside17 = models.FloatField(blank=True, null=True)

    flux_g = models.FloatField(blank=True, null=True)
    flux_ivar_g = models.FloatField(blank=True, null=True)
    flux_r = models.FloatField(blank=True, null=True)
    flux_ivar_r = models.FloatField(blank=True, null=True)
    flux_z = models.FloatField(blank=True, null=True)
    flux_ivar_z = models.FloatField(blank=True, null=True)

    flux_w1 = models.FloatField(blank=True, null=True)
    flux_ivar_w1 = models.FloatField(blank=True, null=True)
    flux_w2 = models.FloatField(blank=True, null=True)
    flux_ivar_w2 = models.FloatField(blank=True, null=True)
    flux_w3 = models.FloatField(blank=True, null=True)
    flux_ivar_w3 = models.FloatField(blank=True, null=True)
    flux_w4 = models.FloatField(blank=True, null=True)
    flux_ivar_w4 = models.FloatField(blank=True, null=True)

    counterparts_number = models.FloatField(blank=True, null=True)
    single_counterpart = models.BooleanField(blank=True, null=True)
    counterparts_type = models.CharField(max_length=100, blank=True, null=True)

    flux_g_ebv = models.FloatField(blank=True, null=True)
    flux_r_ebv = models.FloatField(blank=True, null=True)
    flux_z_ebv = models.FloatField(blank=True, null=True)
    flux_w1_ebv = models.FloatField(blank=True, null=True)
    flux_w2_ebv = models.FloatField(blank=True, null=True)
    flux_w3_ebv = models.FloatField(blank=True, null=True)
    flux_w4_ebv = models.FloatField(blank=True, null=True)

    # xray sources near which gaia source was found
    xray_sources = models.ManyToManyField(eROSITA, related_name='ls_sources', blank=True)
    # File from which source was loaded to system
    origin_file = models.ForeignKey(OriginFile, on_delete=models.CASCADE, related_name='ls_sources', blank=True, null=True)

    # Magnitudes and errors in AB-system to show in Web UI tables
    mag_r_ab = models.FloatField(blank=True, null=True, verbose_name="r")
    mag_err_r_ab = models.FloatField(blank=True, null=True, verbose_name="r err")
    mag_g_ab = models.FloatField(blank=True, null=True, verbose_name="g")
    mag_err_g_ab = models.FloatField(blank=True, null=True, verbose_name="g err")
    mag_z_ab = models.FloatField(blank=True, null=True, verbose_name="z")
    mag_err_z_ab = models.FloatField(blank=True, null=True, verbose_name="z err")
    mag_w1_ab = models.FloatField(blank=True, null=True, verbose_name="w1")
    mag_err_w1_ab = models.FloatField(blank=True, null=True, verbose_name="w1 err")
    mag_w2_ab = models.FloatField(blank=True, null=True, verbose_name="w2")
    mag_err_w2_ab = models.FloatField(blank=True, null=True, verbose_name="w2 err")
    mag_w3_ab = models.FloatField(blank=True, null=True, verbose_name="w3")
    mag_err_w3_ab = models.FloatField(blank=True, null=True, verbose_name="w3 err")
    mag_w4_ab = models.FloatField(blank=True, null=True, verbose_name="w4")
    mag_err_w4_ab = models.FloatField(blank=True, null=True, verbose_name="w4 err")

    def __str__(self):
        return '{} - LS Source: {}'.format(self.opt_hpidx, self.opt_id)

    def __iter__(self):
        for field in self._meta.get_fields():
            value = getattr(self, field.name, "")
            try:
                name = field.verbose_name
            except AttributeError:
                name = field.name

            yield name, value

    class Meta:
        verbose_name_plural = 'LS sources'


# Class for SDSS sources
class SDSS(models.Model):
    opt_id = models.PositiveIntegerField(blank=True, null=True)
    objID = models.CharField(max_length=100, blank=True, null=True)
    ra = models.FloatField(verbose_name='RA')
    dec = models.FloatField(verbose_name='Dec')
    # cartesian coordinates with radius = 1pc
    c_x = models.FloatField(blank=True, null=True)
    c_y = models.FloatField(blank=True, null=True)
    c_z = models.FloatField(blank=True, null=True)
    # add heal pix index for identification
    opt_hpidx = models.BigIntegerField()
    
    RAERR = models.FloatField(blank=True, null=True)
    DECERR = models.FloatField(blank=True, null=True)

    cModelFlux_u = models.FloatField(blank=True, null=True)
    cModelFluxIvar_u = models.FloatField(blank=True, null=True)

    cModelFlux_g = models.FloatField(blank=True, null=True)
    cModelFluxIvar_g = models.FloatField(blank=True, null=True)

    cModelFlux_r = models.FloatField(blank=True, null=True)
    cModelFluxIvar_r = models.FloatField(blank=True, null=True)

    cModelFlux_i = models.FloatField(blank=True, null=True)
    cModelFluxIvar_i = models.FloatField(blank=True, null=True)

    cModelFlux_z = models.FloatField(blank=True, null=True)
    cModelFluxIvar_z = models.FloatField(blank=True, null=True)

    psfFlux_u = models.FloatField(blank=True, null=True)
    psfFluxIvar_u = models.FloatField(blank=True, null=True)

    psfFlux_g = models.FloatField(blank=True, null=True)
    psfFluxIvar_g = models.FloatField(blank=True, null=True)

    psfFlux_r = models.FloatField(blank=True, null=True)
    psfFluxIvar_r = models.FloatField(blank=True, null=True)

    psfFlux_i = models.FloatField(blank=True, null=True)
    psfFluxIvar_i = models.FloatField(blank=True, null=True)

    psfFlux_z = models.FloatField(blank=True, null=True)
    psfFluxIvar_z = models.FloatField(blank=True, null=True)

    cModelMag_u_ab = models.FloatField(blank=True, null=True, verbose_name="u")
    cModelMagErr_u_ab = models.FloatField(blank=True, null=True, verbose_name="u err")
    cModelMag_g_ab = models.FloatField(blank=True, null=True, verbose_name="g")
    cModelMagErr_g_ab = models.FloatField(blank=True, null=True, verbose_name="g err")
    cModelMag_r_ab = models.FloatField(blank=True, null=True, verbose_name="r")
    cModelMagErr_r_ab = models.FloatField(blank=True, null=True, verbose_name="r err")
    cModelMag_i_ab = models.FloatField(blank=True, null=True, verbose_name="i")
    cModelMagErr_i_ab = models.FloatField(blank=True, null=True, verbose_name="i err")
    cModelMag_z_ab = models.FloatField(blank=True, null=True, verbose_name="z")
    cModelMagErr_z_ab = models.FloatField(blank=True, null=True, verbose_name="z err")

    counterparts_number = models.FloatField(blank=True, null=True)
    single_counterpart = models.BooleanField(blank=True, null=True)
    counterparts_type = models.CharField(max_length=100, blank=True, null=True)
    
    # xray sources near which gaia source was found
    xray_sources = models.ManyToManyField(eROSITA, related_name='sdss_sources', blank=True)
    # File from which source was loaded to system
    origin_file = models.ForeignKey(OriginFile, on_delete=models.CASCADE, related_name='sdss_sources', blank=True, null=True)

    def __str__(self):
        return '{} - SDSS Source: {}'.format(self.opt_hpidx, self.opt_id)

    def __iter__(self):
        for field in self._meta.get_fields():
            value = getattr(self, field.name, "")
            try:
                name = field.verbose_name
            except AttributeError:
                name = field.name

            yield name, value

    class Meta:
        verbose_name_plural = 'SDSS sources'


# Class for PS sources
class PS(models.Model):
    opt_id = models.PositiveIntegerField(blank=True, null=True)
    objID = models.CharField(max_length=100, blank=True, null=True)
    ra = models.FloatField()
    dec = models.FloatField()
    # cartesian coordinates with radius = 1pc
    c_x = models.FloatField(blank=True, null=True)
    c_y = models.FloatField(blank=True, null=True)
    c_z = models.FloatField(blank=True, null=True)
    # add heal pix index for identification
    opt_hpidx = models.BigIntegerField()

    raStack = models.FloatField(blank=True, null=True)
    decStack = models.FloatField(blank=True, null=True)
    raStackErr = models.FloatField(blank=True, null=True)
    decStackErr = models.FloatField(blank=True, null=True)
    raMean = models.FloatField(blank=True, null=True)
    decMean = models.FloatField(blank=True, null=True)
    raMeanErr = models.FloatField(blank=True, null=True)
    decMeanErr = models.FloatField(blank=True, null=True)

    objInfoFlag = models.FloatField(blank=True, null=True)
    qualityFlag = models.IntegerField(blank=True, null=True)

    primaryDetection = models.IntegerField(blank=True, null=True)
    bestDetection = models.IntegerField(blank=True, null=True)

    duplicat = models.BooleanField(blank=True, null=True)
    d_to = models.IntegerField(blank=True, null=True)
    fitext = models.BooleanField(blank=True, null=True)
    devaucou = models.BooleanField(blank=True, null=True)
    star = models.BooleanField(blank=True, null=True)

    w1fit = models.BooleanField(blank=True, null=True)
    w1bad = models.BooleanField(blank=True, null=True)
    w1mag = models.FloatField(blank=True, null=True, verbose_name="w1")
    dw1mag = models.FloatField(blank=True, null=True, verbose_name="w1 err")

    w2fit = models.BooleanField(blank=True, null=True)
    w2bad = models.BooleanField(blank=True, null=True)
    w2mag = models.FloatField(blank=True, null=True, verbose_name="w2")
    dw2mag = models.FloatField(blank=True, null=True, verbose_name="w2 err")

    gKronFlux = models.FloatField(blank=True, null=True)
    gKronFluxErr = models.FloatField(blank=True, null=True)
    rKronFlux = models.FloatField(blank=True, null=True)
    rKronFluxErr = models.FloatField(blank=True, null=True)
    iKronFlux = models.FloatField(blank=True, null=True)
    iKronFluxErr = models.FloatField(blank=True, null=True)
    zKronFlux = models.FloatField(blank=True, null=True)
    zKronFluxErr = models.FloatField(blank=True, null=True)
    yKronFlux = models.FloatField(blank=True, null=True)
    yKronFluxErr = models.FloatField(blank=True, null=True)

    gPSFFlux = models.FloatField(blank=True, null=True)
    gPSFFluxErr = models.FloatField(blank=True, null=True)
    rPSFFlux = models.FloatField(blank=True, null=True)
    rPSFFluxErr = models.FloatField(blank=True, null=True)
    iPSFFlux = models.FloatField(blank=True, null=True)
    iPSFFluxErr = models.FloatField(blank=True, null=True)
    zPSFFlux = models.FloatField(blank=True, null=True)
    zPSFFluxErr = models.FloatField(blank=True, null=True)
    yPSFFlux = models.FloatField(blank=True, null=True)
    yPSFFluxErr = models.FloatField(blank=True, null=True)
    
    w1flux = models.FloatField(blank=True, null=True)
    dw1flux = models.FloatField(blank=True, null=True)
    w2flux = models.FloatField(blank=True, null=True)
    dw2flux = models.FloatField(blank=True, null=True)

    counterparts_number = models.FloatField(blank=True, null=True)
    single_counterpart = models.BooleanField(blank=True, null=True)
    counterparts_type = models.CharField(max_length=100, blank=True, null=True)

    # xray sources near which gaia source was found
    xray_sources = models.ManyToManyField(eROSITA, related_name='ps_sources', blank=True)
    # File from which source was loaded to system
    origin_file = models.ForeignKey(OriginFile, on_delete=models.CASCADE, related_name='ps_sources', blank=True,
                                    null=True)

    # Magnitudes and errors in AB-system to show in Web UI tables
    gKronMagAB = models.FloatField(blank=True, null=True, verbose_name="g")
    gKronMagErrAB = models.FloatField(blank=True, null=True, verbose_name="g err")
    rKronMagAB = models.FloatField(blank=True, null=True, verbose_name="r")
    rKronMagErrAB = models.FloatField(blank=True, null=True, verbose_name="r err")
    iKronMagAB = models.FloatField(blank=True, null=True, verbose_name="i")
    iKronMagErrAB = models.FloatField(blank=True, null=True, verbose_name="i err")
    zKronMagAB = models.FloatField(blank=True, null=True, verbose_name="z")
    zKronMagErrAB = models.FloatField(blank=True, null=True, verbose_name="z err")
    yKronMagAB = models.FloatField(blank=True, null=True, verbose_name="y")
    yKronMagErrAB = models.FloatField(blank=True, null=True, verbose_name="y err")

    def __str__(self):
        return '{} - PS Source: {}'.format(self.opt_hpidx, self.opt_id)

    def __iter__(self):
        for field in self._meta.get_fields():
            value = getattr(self, field.name, "")
            try:
                name = field.verbose_name
            except AttributeError:
                name = field.name

            yield name, value

    class Meta:
        verbose_name_plural = 'PS sources'


# Class for GAIA sources
class GAIA(models.Model):
    opt_id = models.PositiveIntegerField(blank=True, null=True)
    objID = models.CharField(max_length=100, blank=True, null=True)
    ra = models.FloatField(verbose_name="RA")
    dec = models.FloatField(verbose_name="Dec")
    # cartesian coordinates with radius = 1pc
    c_x = models.FloatField(blank=True, null=True)
    c_y = models.FloatField(blank=True, null=True)
    c_z = models.FloatField(blank=True, null=True)
    # add heal pix index for identification
    opt_hpidx = models.BigIntegerField()
    # gaia star flag
    star = models.BooleanField(blank=True, null=True)

    ra_error = models.FloatField(blank=True, null=True)
    dec_error = models.FloatField(blank=True, null=True)
    parallax = models.FloatField(blank=True, null=True)
    parallax_error = models.FloatField(blank=True, null=True)
    
    pm = models.FloatField(blank=True, null=True)
    pmra = models.FloatField(blank=True, null=True)
    pmra_error = models.FloatField(blank=True, null=True)
    pmdec = models.FloatField(blank=True, null=True)
    pmdec_error = models.FloatField(blank=True, null=True)

    astrometric_n_good_obs_al = models.IntegerField(blank=True, null=True)
    astrometric_gof_al = models.FloatField(blank=True, null=True)
    astrometric_chi2_al = models.FloatField(blank=True, null=True)
    astrometric_excess_noise = models.FloatField(blank=True, null=True)
    astrometric_excess_noise_sig = models.FloatField(blank=True, null=True)
    
    pseudocolour = models.FloatField(blank=True, null=True)
    pseudocolour_error = models.FloatField(blank=True, null=True)
    
    visibility_periods_used = models.IntegerField(blank=True, null=True)
    ruwe = models.FloatField(blank=True, null=True)
    duplicated_source = models.BooleanField(blank=True, null=True)
    
    phot_g_n_obs = models.IntegerField(blank=True, null=True)
    phot_g_mean_mag = models.FloatField(blank=True, null=True)
    phot_bp_mean_flux = models.FloatField(blank=True, null=True)
    phot_bp_mean_flux_error = models.FloatField(blank=True, null=True)
    phot_bp_mean_mag = models.FloatField(blank=True, null=True)
    phot_rp_mean_flux = models.FloatField(blank=True, null=True)
    phot_rp_mean_flux_error = models.FloatField(blank=True, null=True)
    phot_rp_mean_mag = models.FloatField(blank=True, null=True)
    
    dr2_radial_velocity = models.FloatField(blank=True, null=True)
    dr2_radial_velocity_error = models.FloatField(blank=True, null=True)
    
    l = models.FloatField(blank=True, null=True)
    b = models.FloatField(blank=True, null=True)
    
    ecl_lon = models.FloatField(blank=True, null=True)
    ecl_lat = models.FloatField(blank=True, null=True)
    
    phot_g_mean_flux = models.FloatField(blank=True, null=True)
    phot_g_mean_flux_error = models.FloatField(blank=True, null=True)
    
    counterparts_number = models.FloatField(blank=True, null=True)
    single_counterpart = models.BooleanField(blank=True, null=True)
    counterparts_type = models.CharField(max_length=100, blank=True, null=True)

    # xray sources near which gaia source was found
    xray_sources = models.ManyToManyField(eROSITA, related_name='gaia_sources', blank=True)
    # File from which source was loaded to system
    origin_file = models.ForeignKey(OriginFile, on_delete=models.CASCADE, related_name='gaia_sources', blank=True,
                                    null=True)

    def __str__(self):
        return '{} - GAIA Source: {}'.format(self.opt_hpidx, self.opt_id)

    def __iter__(self):
        for field in self._meta.get_fields():
            value = getattr(self, field.name, "")
            try:
                name = field.verbose_name
            except AttributeError:
                name = field.name

            yield name, value

    class Meta:
        verbose_name_plural = 'GAIA sources'


class AllWise(models.Model):
    ir_id = models.PositiveIntegerField()
    opt_hpidx = models.BigIntegerField()
    designation = models.CharField(max_length=19, blank=True, null=True)
    ra = models.FloatField()
    dec = models.FloatField()
    sigra = models.FloatField(blank=True, null=True)
    sigdec = models.FloatField(blank=True, null=True)
    sigradec = models.FloatField(blank=True, null=True)
    glon = models.FloatField(blank=True, null=True)
    glat = models.FloatField(blank=True, null=True)
    elon = models.FloatField(blank=True, null=True)
    elat = models.FloatField(blank=True, null=True)
    wx = models.FloatField(blank=True, null=True)
    wy = models.FloatField(blank=True, null=True)
    cntr = models.CharField(max_length=19, blank=True, null=True)
    source_id = models.CharField(max_length=20, blank=True, null=True)
    coadd_id = models.CharField(max_length=13, blank=True, null=True)
    src = models.IntegerField(blank=True, null=True)
    w1mpro = models.FloatField(blank=True, null=True)
    w1sigmpro = models.FloatField(blank=True, null=True)
    w1snr = models.FloatField(blank=True, null=True)
    w1rchi2 = models.CharField(max_length=14, blank=True, null=True)
    w2mpro = models.FloatField(blank=True, null=True)
    w2sigmpro = models.FloatField(blank=True, null=True)
    w2snr = models.FloatField(blank=True, null=True)
    w2rchi2 = models.CharField(max_length=13, blank=True, null=True)
    w3mpro = models.FloatField(blank=True, null=True)
    w3sigmpro = models.FloatField(blank=True, null=True)
    w3snr = models.FloatField(blank=True, null=True)
    w3rchi2 = models.CharField(max_length=13, blank=True, null=True)
    w4mpro = models.FloatField(blank=True, null=True)
    w4sigmpro = models.FloatField(blank=True, null=True)
    w4snr = models.FloatField(blank=True, null=True)
    w4rchi2 = models.CharField(max_length=12, blank=True, null=True)
    rchi2 = models.FloatField(blank=True, null=True)
    nb = models.IntegerField(blank=True, null=True)
    na = models.IntegerField(blank=True, null=True)
    w1sat = models.FloatField(blank=True, null=True)
    w2sat = models.FloatField(blank=True, null=True)
    w3sat = models.FloatField(blank=True, null=True)
    w4sat = models.FloatField(blank=True, null=True)
    satnum = models.CharField(max_length=4, blank=True, null=True)
    ra_pm = models.FloatField(blank=True, null=True)
    dec_pm = models.FloatField(blank=True, null=True)
    sigra_pm = models.FloatField(blank=True, null=True)
    pmdec = models.IntegerField(blank=True, null=True)
    sigpmdec = models.IntegerField(blank=True, null=True)
    cc_flags = models.CharField(max_length=4, blank=True, null=True)
    rel = models.CharField(max_length=4, blank=True, null=True)
    ext_flg = models.IntegerField(blank=True, null=True)
    var_flg = models.CharField(max_length=4, blank=True, null=True)
    ph_qual = models.CharField(max_length=4, blank=True, null=True)
    det_bit = models.IntegerField(blank=True, null=True)
    moon_lev = models.CharField(max_length=4, blank=True, null=True)
    w1nm = models.IntegerField(blank=True, null=True)
    w1m = models.IntegerField(blank=True, null=True)
    w2nm = models.IntegerField(blank=True, null=True)
    w2m = models.IntegerField(blank=True, null=True)
    w3nm = models.IntegerField(blank=True, null=True)
    w3m = models.IntegerField(blank=True, null=True)
    w4nm = models.IntegerField(blank=True, null=True)
    w4m = models.IntegerField(blank=True, null=True)
    best_use_cntr = models.CharField(max_length=19, blank=True, null=True)
    w1cov = models.FloatField(blank=True, null=True)
    w2cov = models.FloatField(blank=True, null=True)
    w3cov = models.FloatField(blank=True, null=True)
    w4cov = models.FloatField(blank=True, null=True)
    w1cc_map = models.IntegerField(blank=True, null=True)
    w1cc_map_str = models.CharField(max_length=4, blank=True, null=True)
    w2cc_map = models.IntegerField(blank=True, null=True)
    w2cc_map_str = models.CharField(max_length=4, blank=True, null=True)
    w3cc_map = models.IntegerField(blank=True, null=True)
    w3cc_map_str = models.CharField(max_length=4, blank=True, null=True)
    w4cc_map = models.IntegerField(blank=True, null=True)
    w4cc_map_str = models.CharField(max_length=4, blank=True, null=True)
    tmass_key = models.CharField(max_length=10, blank=True, null=True)
    r_2mass = models.FloatField(blank=True, null=True)
    pa_2mass = models.FloatField(blank=True, null=True)
    n_2mass = models.IntegerField(blank=True, null=True)
    j_m_2mass = models.FloatField(blank=True, null=True)
    j_msig_2mass = models.FloatField(blank=True, null=True)
    h_m_2mass = models.FloatField(blank=True, null=True)
    h_msig_2mass = models.FloatField(blank=True, null=True)
    k_m_2mass = models.FloatField(blank=True, null=True)
    k_msig_2mass = models.FloatField(blank=True, null=True)
    x = models.FloatField(blank=True, null=True)
    y = models.FloatField(blank=True, null=True)
    z = models.FloatField(blank=True, null=True)
    spt_ind = models.CharField(max_length=9, blank=True, null=True)
    htm20 = models.CharField(max_length=14, blank=True, null=True)

    def __str__(self):
        return '{} - ALLWISE Source: {}'.format(self.opt_hpidx, self.ir_id)

    def __iter__(self):
        for field in self._meta.get_fields():
            value = getattr(self, field.name, "")
            try:
                name = field.verbose_name
            except AttributeError:
                name = field.name

            yield name, value

    class Meta:
        verbose_name_plural = 'GAIA sources'

    # TODO -- побить на файлы каждый для своего обзора и доработать команду для загрузки данных
    # TODO -- потом дорабатываем интерфейс


# Class for comments on optical data
class OptComment(models.Model):
    comment = models.TextField(max_length=1500)
    follow_up = models.TextField(max_length=500, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='opt_comments')
    updated_at = models.DateTimeField(blank=True, null=True)

    meta_source = models.ForeignKey(MetaObject, on_delete=models.CASCADE, related_name='opt_comments', blank=True, null=True)

    def __str__(self):
        truncated_comment = Truncator(self.comment)
        return 'OptComment: {}'.format(truncated_comment.chars(10))

    class Meta:
        ordering = ('-created_by__is_superuser',)


class MetaObjFilterBookmark(models.Model):
    """A model for meta sources filtering (a.k.a. custom preclass)."""
    name = models.CharField(max_length=128, primary_key=True)
    author = models.ForeignKey(User, blank=False,
                               null=True, on_delete=models.SET_NULL)
    criteria = models.TextField(null=False)
    description = models.TextField(blank=True, null=False)

    def __str__(self):
        return (
            f"Custom Sources Filter:\n"
            f"\tName:        {self.name}\n"
            f"\tAuthor:      {str(self.author)}\n"
            f"\tGET Request: {self.human_readable_criteria}\n"
            f"\tDescription: {self.description}\n"
        )

    @property
    def human_readable_criteria(self):
        """Replaces Django Queryset API operations with human-readable
        math signs."""
        criteria = self.criteria

        queryset_api_to_signs = {
            "__gt=": " > ",
            "__ge=": " >= ",
            "__lt=": " < ",
            "__le=": " <= ",
            "=": " = ",
            "&": " AND ",
        }

        for api_from, sign_to in queryset_api_to_signs.items():
            criteria = re.sub(
                rf"[^ ]{api_from}[^ ]",
                lambda match: match[0][0] + sign_to + match[0][-1],
                criteria
            )

        return criteria

    @staticmethod
    def parse_url(url: str) -> str:
        """Cuts GET request from url and cleans from redundant parts."""
        try:
            url_addr, get_request = re.findall(r"^(.*)?/\?(.*)$", url)[0]
        except IndexError:
            raise ValueError(f"Invalid url with get request: {url}")

        get_request = re.split("&", get_request)
        get_request = [clause for clause in get_request
                       if re.fullmatch(r"[^=]*?=[^=]+", clause) is not None]

        if len(get_request) == 0:
            raise ValueError(f"Url has either empty of redundant get request: "
                             f"{url}")

        return "&".join(get_request)
