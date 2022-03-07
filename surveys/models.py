from django.db import models
from django.utils.text import Truncator
from django.contrib.auth.models import User
from itertools import zip_longest
from django.db.models import Max


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
    master_survey = models.PositiveIntegerField(blank=True, null=True)
    RA = models.FloatField()
    DEC = models.FloatField()

    unchange_flag = models.BooleanField(default=False, blank=True, null=True)

    comment = models.TextField(max_length=2000, blank=True, null=True)

    # TODO: change this later
    CLASS_CHOICES = [
        ('TDE: Criteria 1', 'TDE: Parameter A > a1, Parameter B > b2'),
        ('TDE: Criteria 2', 'TDE: Parameter B > b1, Parameter C > c2'),
        ('TDE: Criteria 3', 'TDE: Parameter D > d1, Parameter E > e2'),
        ('NOT TDE: Criteria 1', 'Not TDE: Parameter A < a1, Parameter B < b2'),
        ('NOT TDE: Criteria 2', 'Not TDE: Parameter B < b1, Parameter C < c2'),
        ('NOT TDE: Criteria 3', 'Not TDE: Parameter D < d1, Parameter E < e2'),
        (None, 'Unknown Source'),
    ]
    object_class = models.CharField(
        max_length=100,
        choices=CLASS_CHOICES,
        default=None,
        blank=True, null=True,
    )

    # Columns of Master Table
    EXT = models.FloatField(blank=True, null=True)
    R98 = models.FloatField(blank=True, null=True)
    LIKE = models.FloatField(blank=True, null=True)

    # TODO: think about next surveys: 5,6,7,8
    D2D_e1m = models.FloatField(blank=True, null=True)
    D2D_e2m = models.FloatField(blank=True, null=True)
    D2D_e3m = models.FloatField(blank=True, null=True)
    D2D_e4m = models.FloatField(blank=True, null=True)
    D2D_me1 = models.FloatField(blank=True, null=True)
    D2D_me2 = models.FloatField(blank=True, null=True)
    D2D_me3 = models.FloatField(blank=True, null=True)
    D2D_me4 = models.FloatField(blank=True, null=True)

    EXP_e1 = models.FloatField(blank=True, null=True)
    EXP_e2 = models.FloatField(blank=True, null=True)
    EXP_e3 = models.FloatField(blank=True, null=True)
    EXP_e4 = models.FloatField(blank=True, null=True)
    EXP_e1234 = models.FloatField(blank=True, null=True)

    # TODO: do we need these fields???
    ID_FLAG_e1m = models.IntegerField(blank=True, null=True)
    ID_FLAG_e2m = models.IntegerField(blank=True, null=True)
    ID_FLAG_e3m = models.IntegerField(blank=True, null=True)
    ID_FLAG_e4m = models.IntegerField(blank=True, null=True)
    ID_FLAG_me1 = models.IntegerField(blank=True, null=True)
    ID_FLAG_me2 = models.IntegerField(blank=True, null=True)
    ID_FLAG_me3 = models.IntegerField(blank=True, null=True)
    ID_FLAG_me4 = models.IntegerField(blank=True, null=True)

    ID_e1 = models.BigIntegerField(blank=True, null=True)
    ID_e2 = models.BigIntegerField(blank=True, null=True)
    ID_e3 = models.BigIntegerField(blank=True, null=True)
    ID_e4 = models.BigIntegerField(blank=True, null=True)
    ID_e1234 = models.BigIntegerField(blank=True, null=True)

    RATIO_e2e1 = models.FloatField(blank=True, null=True)
    RATIO_e3e2 = models.FloatField(blank=True, null=True)
    RATIO_e4e3 = models.FloatField(blank=True, null=True)

    RFLAG_e2e1 = models.IntegerField(blank=True, null=True)
    RFLAG_e3e2 = models.IntegerField(blank=True, null=True)
    RFLAG_e4e3 = models.IntegerField(blank=True, null=True)

    R_NSRC_e1m = models.IntegerField(blank=True, null=True)
    R_NSRC_e2m = models.IntegerField(blank=True, null=True)
    R_NSRC_e3m = models.IntegerField(blank=True, null=True)
    R_NSRC_e4m = models.IntegerField(blank=True, null=True)
    R_NSRC_me1 = models.IntegerField(blank=True, null=True)
    R_NSRC_me2 = models.IntegerField(blank=True, null=True)
    R_NSRC_me3 = models.IntegerField(blank=True, null=True)
    R_NSRC_me4 = models.IntegerField(blank=True, null=True)

    UPLIM_e1 = models.FloatField(blank=True, null=True)
    UPLIM_e2 = models.FloatField(blank=True, null=True)
    UPLIM_e3 = models.FloatField(blank=True, null=True)
    UPLIM_e4 = models.FloatField(blank=True, null=True)
    UPLIM_e1234 = models.FloatField(blank=True, null=True)

    flag = models.IntegerField(blank=True, null=True)

    # TSTART_e1,2,3,4
    TSTART_e1 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e2 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e3 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e4 = models.CharField(max_length=100, blank=True, null=True)
    # TSTOP_e1,2,3,4
    TSTOP_e1 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e2 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e3 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e4 = models.CharField(max_length=100, blank=True, null=True)

    primary_object = models.BooleanField(default=True, blank=True, null=True)
    # Meta Group object for meta objects with common sources
    meta_group = models.ForeignKey(MetaGroup, on_delete=models.CASCADE, related_name='meta_objects', blank=True, null=True)

    def __str__(self):
        return '{} - MetaObject: {}'.format(self.meta_ind, self.master_name)

    @staticmethod
    def fields_to_show():
        fields = ['meta_ind', 'master_name', 'master_survey', 'RA', 'DEC', 'unchange_flag', 'comment', 'object_class', 'EXT', 'R98', 'LIKE',
                  'D2D_e1m', 'D2D_e2m', 'D2D_e3m', 'D2D_e4m', 'D2D_me1', 'D2D_me2', 'D2D_me3', 'D2D_me4',
                  'EXP_e1', 'EXP_e2', 'EXP_e3', 'EXP_e4', 'EXP_e1234',
                  'ID_FLAG_e1m', 'ID_FLAG_e2m', 'ID_FLAG_e3m', 'ID_FLAG_e4m',
                  'ID_FLAG_me1', 'ID_FLAG_me2', 'ID_FLAG_me3', 'ID_FLAG_me4',
                  'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4', 'ID_e1234',
                  'RATIO_e2e1', 'RATIO_e3e2', 'RATIO_e4e3', 'RFLAG_e2e1', 'RFLAG_e3e2', 'RFLAG_e4e3',
                  'R_NSRC_e1m', 'R_NSRC_e2m', 'R_NSRC_e3m', 'R_NSRC_e4m',
                  'R_NSRC_me1', 'R_NSRC_me2', 'R_NSRC_me3', 'R_NSRC_me4',
                  'UPLIM_e1', 'UPLIM_e2', 'UPLIM_e3', 'UPLIM_e4', 'UPLIM_e1234', 'flag',
                  'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTART_e4', 'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3', 'TSTOP_e4']
        return fields

    def find_master_source(self):
        sources = self.object_sources.all()
        if sources:
            max_dl0 = sources.aggregate(Max('DET_LIKE_0'))['DET_LIKE_0__max']
            master_source = sources.get(DET_LIKE_0=max_dl0)
            print(f'Found master_source  - {master_source.name} with DET_LIKE_0: {max_dl0}')
            # take name, survey, RA, DEC, EXT, R98, LIKE from master_source
            self.master_name = master_source.name
            self.master_survey = master_source.survey.name
            # TODO: uncomment later
            # self.RA = master_source.RA
            # self.DEC = master_source.DEC
            # self.EXT = master_source.EXT
            # self.R98 = master_source.pos_r98
            # self.LIKE = master_source.DET_LIKE_0
            self.save()

    def __iter__(self):
        for field in MetaObject.fields_to_show():
            value = getattr(self, field, None)
            yield field, value


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

    # master_source = models.BooleanField(default=True, blank=True, null=True)

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
    # meta_object = models.ForeignKey(MetaObject, on_delete=models.CASCADE, related_name='object_sources', blank=True, null=True)
    meta_objects = models.ManyToManyField(MetaObject, related_name='object_sources', blank=True)

    def __str__(self):
        return '{} - Source: {}'.format(self.survey_ind, self.name)

    def get_comment_count(self):
        return Comment.objects.filter(source=self).count()

    def get_last_comment(self):
        return Comment.objects.filter(source=self).order_by('-created_at').first()

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

    def __iter__(self):
        for field in eROSITA.fields_to_show():
            value = getattr(self, field, None)
            yield field, value

    class Meta:
        verbose_name_plural = 'eROSITA sources'


# Class for comments on xray sources
class Comment(models.Model):
    comment = models.TextField(max_length=2000)
    follow_up = models.TextField(max_length=1000, blank=True, null=True)

    source_class = models.CharField(
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


# Class for GAIA sources
class GAIA(models.Model):
    name = models.CharField(max_length=150)

    # TODO: change this later to FloatField
    RA = models.DecimalField(max_digits=9, decimal_places=5)
    DEC = models.DecimalField(max_digits=9, decimal_places=5)

    # xray sources near which gaia source was found
    xray_sources = models.ManyToManyField(eROSITA, related_name='gaia_sources')
    # File from which source was loaded to system
    origin_file = models.ForeignKey(OriginFile, on_delete=models.CASCADE, related_name='gaia_sources', blank=True, null=True)
    # Meta object for which this gaia source is master
    meta_object = models.OneToOneField(MetaObject, on_delete=models.CASCADE, related_name='master_gaia', blank=True, null=True)

    def __str__(self):
        return 'GAIA Source: {}'.format(self.name)

    class Meta:
        verbose_name_plural = 'GAIA sources'


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
