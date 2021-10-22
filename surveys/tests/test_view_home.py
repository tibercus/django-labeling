from django.test import TestCase
from django.urls import resolve, reverse

from ..models import *
from ..views import home


class HomeTests(TestCase):
    def setUp(self):
        survey = Survey.objects.create(name=1, description='First Test Survey.')
        self.source = Source.objects.create(name="SGE_test", RA=0, DEC=0, dup_id=2, survey=survey)
        url = reverse('home')
        self.response = self.client.get(url)

    def test_home_view_status_code(self):
        self.assertEquals(self.response.status_code, 200)

    def test_home_url_resolves_home_view(self):
        view = resolve('/')
        self.assertEquals(view.func, home)

    def test_home_view_contains_link_to_source_page(self):
        source_url = reverse('source', kwargs={'pk': self.source.pk})
        self.assertContains(self.response, 'href="{0}"'.format(source_url))