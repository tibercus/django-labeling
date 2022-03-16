from django.contrib import admin
from import_export import resources
from .models import *
from .forms import CustomImportForm # CustomConfirmImportForm
from import_export.admin import ImportMixin
from import_export.fields import Field
from import_export.widgets import ForeignKeyWidget


class SourceInline(admin.TabularInline):
    model = eROSITA.meta_objects.through


class MetaAdmin(admin.ModelAdmin):
    readonly_fields = ('id',)
    inlines = [
        SourceInline,
    ]


class ObjectInline(admin.TabularInline):
    model = MetaObject


class MetaGroupAdmin(admin.ModelAdmin):
    readonly_fields = ('id',)
    inlines = [
       ObjectInline,
    ]


class LSAdmin(admin.ModelAdmin):
    readonly_fields = ('id',)
    filter_horizontal = ('xray_sources',)


class LSInline(admin.TabularInline):
    model = LS.xray_sources.through


class SDSSInline(admin.TabularInline):
    model = SDSS.xray_sources.through


class PSInline(admin.TabularInline):
    model = PS.xray_sources.through


class eROSITAAdmin(admin.ModelAdmin):
    readonly_fields = ('id',)
    filter_horizontal = ('meta_objects',)
    inlines = [
        LSInline, SDSSInline, PSInline
    ]


admin.site.register(MetaGroup, MetaGroupAdmin)
admin.site.register(MetaObject, MetaAdmin)
admin.site.register(OriginFile)
admin.site.register(Survey)

admin.site.register(eROSITA, eROSITAAdmin)
admin.site.register(Comment)
admin.site.register(LS, LSAdmin)
admin.site.register(SDSS, LSAdmin)
admin.site.register(PS, LSAdmin)
admin.site.register(OptComment)

