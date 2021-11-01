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
    name = models.CharField(max_length=255, unique=True)
    RA = models.DecimalField(max_digits=9, decimal_places=4)
    DEC = models.DecimalField(max_digits=9, decimal_places=4)
    ztf_name = models.CharField(max_length=255, blank=True, null=True)
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

    master_source = models.BooleanField(default=False, blank=True, null=True)

    dup_id = models.PositiveIntegerField(blank=True, null=True)
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



