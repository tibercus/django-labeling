from django.contrib import admin
from import_export import resources
from .models import *
from .forms import CustomImportForm # CustomConfirmImportForm
from import_export.admin import ImportMixin
from import_export.fields import Field
from import_export.widgets import ForeignKeyWidget


class eROSITAAdmin(admin.ModelAdmin):
    readonly_fields = ('id',)
    filter_horizontal = ('meta_objects',)


class SourceInline(admin.TabularInline):
    model = eROSITA.meta_objects.through


class MetaAdmin(admin.ModelAdmin):
    readonly_fields = ('id',)
    inlines = [
        SourceInline,
    ]


admin.site.register(MetaObject, MetaAdmin)
admin.site.register(OriginFile)
admin.site.register(Survey)

admin.site.register(eROSITA, eROSITAAdmin)
admin.site.register(Comment)
admin.site.register(GAIA)
admin.site.register(OptComment)

