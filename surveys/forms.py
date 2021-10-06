from django import forms
from import_export.forms import ImportForm, ConfirmImportForm
from .models import Survey

class CustomImportForm(ImportForm):
    survey = forms.ModelChoiceField(
        queryset=Survey.objects.all(),
        empty_label=None,
        required=True)

# class CustomConfirmImportForm(ConfirmImportForm):
#     survey = forms.ModelChoiceField(
#         queryset=Survey.objects.all(),
#         required=True)