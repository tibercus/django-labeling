from unittest import skip

from django.test import TestCase
from django.urls import resolve, reverse
from django.contrib.auth.models import User

from ..models import *
from ..views import home, source
from ..forms import NewCommentForm

@skip("Broken test (what is Source?)")
class SourceTests(TestCase):
    def setUp(self):
        survey = Survey.objects.create(name=1, description='First Test Survey.')
        self.source = Source.objects.create(name="SGE_test", RA=0, DEC=0, dup_id=2, survey=survey)
        User.objects.create_user(username='john', password='Astro123')
        self.client.login(username='john', password='Astro123')

    def test_source_view_status_code(self):
        url = reverse('source', kwargs={'pk': self.source.pk})
        response = self.client.get(url)
        self.assertEquals(response.status_code, 200)

    def test_csrf(self):
        url = reverse('source', kwargs={'pk': self.source.pk})
        response = self.client.get(url)
        self.assertContains(response, 'csrfmiddlewaretoken')

    def test_source_valid_comment_data(self):
        url = reverse('source', kwargs={'pk': self.source.pk})
        data = {
            'comment': 'Lorem ipsum dolor sit amet',
            'follow_up': 'test follow up',
            'source_id': self.source.pk   # this is hidden field for using nav-tabs
        }
        response = self.client.post(url, data)
        self.assertTrue(Comment.objects.exists())

    def test_contains_form(self):
        url = reverse('source', kwargs={'pk': self.source.pk})
        response = self.client.get(url)
        form = response.context.get('form')
        self.assertIsInstance(form, NewCommentForm)

    # TODO: When will have comments logic done
    # def test_source_invalid_comment_data(self):
    #     '''
    #     Invalid post data should not redirect
    #     The expected behavior is to show the form again with validation errors
    #     '''
    #     url = reverse('source', kwargs={'pk': self.source.pk})
    #     response = self.client.post(url, {})
    #     form = response.context.get('form')
#         self.assertEquals(response.status_code, 200)
#         self.assertTrue(form.errors)

    # def test_source_invalid_comment_data_empty_fields(self):
    #     '''
    #     Invalid post data should not redirect
    #     The expected behavior is to show the form again with validation errors
    #     '''
    #     url = reverse('source', kwargs={'pk': self.source.pk})
    #     data = {
    #         'comment': '',
    #         'follow_up': ''
    #     }
    #     response = self.client.post(url, data)
    #     self.assertEquals(response.status_code, 200)
    #     self.assertFalse(Comment.objects.exists())

