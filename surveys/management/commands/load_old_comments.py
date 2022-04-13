from django.core.management import BaseCommand, CommandError
from django.utils import timezone
import pandas as pd
import numpy as np

from surveys.models import *
from django.contrib.auth.models import User
from django.conf import settings
import shutil
import os

import astropy.units as u
from astropy.coordinates import SkyCoord
from django.db.models import Q

import astropy_healpix
from astropy_healpix import HEALPix


class Command(BaseCommand):
    help = "Load old comments."

    def handle(self, *args, **options):
        start_time = timezone.now()
        file_path = os.path.join(settings.WORK_DIR, 'ID_notes.csv')
        comments = pd.read_csv(file_path)
        comments.reset_index(drop=True, inplace=True)
        # comments.drop_duplicates(subset=['RA', 'DEC'], inplace=True)
        print(f'Load old comments: {comments}')

        archive_user, created = User.objects.get_or_create(username='archive')
        if created:
            print(f'Create archive user: {archive_user}')

        for index, row in comments.iterrows():
            # filter meta objects in 1x1 square
            ra_r = round(row['RA'], 1)
            dec_r = round(row['DEC'], 1)
            print(f"\n{index} - Look for meta object for comment  ra: {row['RA']}, dec: {row['DEC']}")
            meta_objects = MetaObject.objects.filter(Q(RA__gt=(ra_r-0.2)), Q(RA__lt=(ra_r+0.2)), Q(DEC__gt=(dec_r-0.2)), Q(DEC__lt=(dec_r+0.2)))
            if not meta_objects.exists():
                print(f"No Meta Objects for comment\n")
            else:
                meta_ra = meta_objects.values_list('RA', flat=True)
                meta_dec = meta_objects.values_list('DEC', flat=True)
                # to get obj by index
                meta_object_arr = np.array(list(meta_objects))
                # get coords of all meta objects
                coords = SkyCoord(meta_ra * u.deg, meta_dec * u.deg, frame='icrs')
                # get coord of comment's source
                c = SkyCoord(row['RA'] * u.deg, row['DEC'] * u.deg, frame='icrs')
                seps = c.separation(coords).arcsecond
                seps_30 = seps[seps < 30]
                # get separations less that 30 arcsec
                if seps_30.size == 0:
                    print(f"No Meta Objects in 30'' for comment")
                    nearest_obj = meta_object_arr[np.argmin(seps)]
                    print(f'Nearest sep: {min(seps)} for object ra: {nearest_obj.RA}, dec: {nearest_obj.DEC}\n')
                else:
                    nearest_obj = meta_object_arr[np.argmin(seps)]
                    print(f'Nearest sep: {min(seps)} for object: {nearest_obj} ra: {nearest_obj.RA}, dec: {nearest_obj.DEC}')
                    if not nearest_obj.primary_object:
                        group = nearest_obj.meta_group
                        nearest_obj = group.meta_objects.get(primary_object=True)
                        print(f'Nearest primary object: {nearest_obj} ra: {nearest_obj.RA}, dec: {nearest_obj.DEC}')

                    # create comment
                    self.stdout.write(f"NOTE:Create comment for object {nearest_obj} by {archive_user}")
                    com_text = 'Comment: ' + str(row['notes']) + ' ' + 'Class: ' + str(row['ID'])
                    comment = Comment.objects.create(comment=com_text, created_by=archive_user, meta_source=nearest_obj)

        self.stdout.write(f'End loading comments')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Loading took: {(end_time-start_time).total_seconds()} seconds.'))
