from django.db import models
from django.utils.text import Truncator
from django.contrib.auth.models import User

class Survey(models.Model):
    name = models.PositiveIntegerField(unique=True)
    started_at = models.DateTimeField(auto_now_add=True)
    ended_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField(max_length=1000)

    def __str__(self):
        return 'Survey {}'.format(self.name)

    def get_sources_count(self):
        return Source.objects.filter(survey=self).count()

class Source(models.Model):
    name = models.CharField(max_length=150, unique=True)
    #  for sources and their measurements in surveys
    RA = models.DecimalField(max_digits=9, decimal_places=5)
    DEC = models.DecimalField(max_digits=9, decimal_places=5)

    ztf_name = models.CharField(max_length=100, blank=True, null=True)
    comment = models.TextField(max_length=2000, blank=True, null=True)

    CLASS_CHOICES = [
        ('TDE', 'TDE Source'),
        ('NOT TDE', 'Not TDE Source'),
        ('NaN', 'Unknown Source'),
    ]
    source_class = models.CharField(
        max_length=20,
        choices=CLASS_CHOICES,
        default='NaN',
        blank=True, null=True,
    )

    master_source = models.BooleanField(default=True, blank=True, null=True)
    dup_id = models.PositiveIntegerField(blank=True, null=True)

    # TODO: Think about formats and view of NEW fields
    L = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    B = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    R98 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    g_d2d = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    g_s = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    g_nsrc = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    g_gmag = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    s_d2d = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    s_id = models.CharField(max_length=100, blank=True, null=True)
    s_z = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    s_otype = models.CharField(max_length=100, blank=True, null=True)
    # w1,2,3
    w_e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    w_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    w_e3 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)

    # w1_snr,2,3
    w_snr_e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    w_snr_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    w_snr_e3 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)

    w_nsrc = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    flag_agn_wise = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    flag_xray = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    flag_radio = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    sdss_p = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    sdss_nsrc = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    RATIO_e2e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    # FLUX_e2,3,4
    FLUX_e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    FLUX_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    FLUX_e3 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)

    # CTS_e2,3,4
    CTS_e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    CTS_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    CTS_e3 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)

    # EXP_e2,3,4
    EXP_e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    EXP_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    EXP_e3 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    # LIKE_e2,3,4
    LIKE_e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    LIKE_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    LIKE_e3 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)

    G_L_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    G_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    G_U_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    Tin_L_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    Tin_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    Tin_U_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    NH_L_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    NH_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    NH_U_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    # UPLIM_e1,2,3,4
    UPLIM_e1 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    UPLIM_e2 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)
    UPLIM_e3 = models.DecimalField(max_digits=9, decimal_places=5, blank=True, null=True)

    # TSTART_e1,2,3,4
    TSTART_e1 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e2 = models.CharField(max_length=100, blank=True, null=True)
    TSTART_e3 = models.CharField(max_length=100, blank=True, null=True)

    # TSTOP_e1,2,3,4
    TSTOP_e1 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e2 = models.CharField(max_length=100, blank=True, null=True)
    TSTOP_e3 = models.CharField(max_length=100, blank=True, null=True)
    # END of new fields

    survey = models.ForeignKey(Survey, on_delete=models.CASCADE, related_name='sources')

    def __str__(self):
        return 'Source: {}'.format(self.name)

    def get_comment_count(self):
        return Comment.objects.filter(source=self).count()

    def get_last_comment(self):
        return Comment.objects.filter(source=self).order_by('-created_at').first()

    def __iter__(self):
        f_list = ['comments', 'master_source', 'survey']  # TODO: how to not show fields smarter
        for field in self._meta.get_fields():
            if field.name not in f_list:
                value = getattr(self, field.name, None)
                yield (field.name, value)

class Comment(models.Model):
    comment = models.TextField(max_length=2000)
    follow_up = models.TextField(max_length=1000, blank=True, null=True)

    source_class = models.CharField(
        max_length=20,
        choices=Source.CLASS_CHOICES,
        default='NaN',
        blank=True, null=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='comments')
    updated_at = models.DateTimeField(blank=True, null=True)

    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name='comments')

    def __str__(self):
        truncated_comment = Truncator(self.comment)
        return 'Comment: {}'.format(truncated_comment.chars(10))



