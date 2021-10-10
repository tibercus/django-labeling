from django import forms
from import_export.forms import ImportForm, ConfirmImportForm
from .models import Survey, Source, Comment

class CustomImportForm(ImportForm):
    survey = forms.ModelChoiceField(
        queryset=Survey.objects.all(),
        empty_label=None,
        required=True)

class NewCommentForm(forms.ModelForm):

    class Meta:
        model = Comment
        fields = ['comment', 'follow_up', 'source_class']