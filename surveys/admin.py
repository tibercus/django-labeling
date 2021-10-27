from django.contrib import admin
from import_export import resources
from .models import *
from .forms import CustomImportForm # CustomConfirmImportForm
from import_export.admin import ImportMixin
from import_export.fields import Field

class SourceResource(resources.ModelResource):
    dup_id = Field(attribute='dup_id', column_name='id')

    def __init__(self, survey=None):
        super()
        self.survey = survey

    # Insert the survey into each row
    def before_import_row(self, row, **kwargs):
        row['survey'] = self.survey

    class Meta:
        model = Source
        import_id_fields = ('name', 'RA', 'DEC',)
        exclude = ('id',)
        skip_unchanged = True
        fields = ('name', 'RA', 'DEC', 'ztf_name', 'source_class', 'survey')


class CustomSourceAdmin(ImportMixin, admin.ModelAdmin):
    resource_class = SourceResource
    list_display = ('dup_id', 'name', 'RA', 'DEC', 'ztf_name', 'comment', 'source_class', 'survey')

    def get_import_form(self):
        return CustomImportForm

    def get_resource_kwargs(self, request, *args, **kwargs):
        rk = super().get_resource_kwargs(request, *args, **kwargs)
        # This method may be called by the initial form GET request, before
        # the survey is chosen. So we default to None.
        rk['survey'] = None
        if request.POST:  # *Now* we should have a non-null value
            # In the dry-run import, the survey is included as a form field.
            survey = request.POST.get('survey', None)
            if survey:
                # If we have it, save it in the session so we have it for the confirmed import.
                request.session['survey'] = survey
            else:
                try:
                    # If we don't have it from a form field, we should find it in the session.
                    survey = request.session['survey']
                except KeyError as e:
                    raise Exception("Context failure on row import, " +
                                    f"check admin.py for more info: {e}")
            rk['survey'] = survey
        return rk

admin.site.register(Survey)
admin.site.register(Comment)
admin.site.register(Source, CustomSourceAdmin)

