from django import forms
from import_export.forms import ImportForm, ConfirmImportForm
from .models import *


class CustomImportForm(ImportForm):
    survey = forms.ModelChoiceField(
            queryset=Survey.objects.all(),
            empty_label=None,
            required=True)


class NewCommentForm(forms.ModelForm):
    source_class = forms.ChoiceField(choices=MetaObject.CLASS_CHOICES, required=False, label='Class 1', initial='')
    source_class_1 = forms.ChoiceField(choices=MetaObject.CLASS_CHOICES, required=False, label='Class 2', initial='')
    source_class_2 = forms.ChoiceField(choices=MetaObject.CLASS_CHOICES, required=False, label='Class 3', initial='')

    comment = forms.CharField(
        widget=forms.Textarea(),
        max_length=2000,
        help_text='The max length of the text is 2000.'
    )

    class Meta:
        model = Comment
        fields = ['source_class', 'source_class_1', 'source_class_2', 'comment', 'follow_up']


class OptCommentForm(forms.ModelForm):
    comment = forms.CharField(
        widget=forms.Textarea(),
        max_length=1500,
        help_text='The max length of the text is 1500.'
    )

    class Meta:
        model = OptComment
        fields = ['comment', 'follow_up']


class OptCounterpartForm(forms.Form):
    def __init__(self, opt_sources_ids=None, *args, **kwargs):
        super(OptCounterpartForm, self).__init__(*args, **kwargs)
        if opt_sources_ids:
            self.fields['cp_id'] = forms.ChoiceField(choices=[ (opt_id, str(opt_id)) for opt_id in opt_sources_ids ], label='Counterpart id')