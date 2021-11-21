from django import forms
from import_export.forms import ImportForm, ConfirmImportForm
from .models import Survey, Comment, OptComment


class CustomImportForm(ImportForm):
    survey = forms.ModelChoiceField(
            queryset=Survey.objects.all(),
            empty_label=None,
            required=True)


class NewCommentForm(forms.ModelForm):
    comment = forms.CharField(
        widget=forms.Textarea(),
        max_length=2000,
        help_text='The max length of the text is 2000.'
    )

    class Meta:
        model = Comment
        fields = ['comment', 'follow_up', 'source_class']


class OptCommentForm(forms.ModelForm):
    comment = forms.CharField(
        widget=forms.Textarea(),
        max_length=1500,
        help_text='The max length of the text is 1500.'
    )

    class Meta:
        model = OptComment
        fields = ['comment', 'follow_up']