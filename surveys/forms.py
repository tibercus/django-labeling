from django import forms
from import_export.forms import ImportForm, ConfirmImportForm
from .models import *


class CustomImportForm(ImportForm):
    survey = forms.ModelChoiceField(
            queryset=Survey.objects.all(),
            empty_label=None,
            required=True)


class NewCommentForm(forms.ModelForm):
    source_class = forms.ChoiceField(choices=MetaObject.CLASS_CHOICES, required=False, label='Class', initial='')

    comment = forms.CharField(
        widget=forms.Textarea(),
        max_length=2000,
        help_text='The max length of the text is 2000.'
    )

    class Meta:
        model = Comment
        fields = ['source_class', 'comment', 'follow_up']


class OptCommentForm(forms.ModelForm):
    comment = forms.CharField(
        widget=forms.Textarea(),
        max_length=1500,
        help_text='The max length of the text is 1500.'
    )

    class Meta:
        model = OptComment
        fields = ['comment', 'follow_up']