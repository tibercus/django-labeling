from django.core.management import BaseCommand, CommandError
from django.utils import timezone
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from surveys.models import *
from django.conf import settings
import shutil
import os

import astropy.units as u
from astropy.coordinates import SkyCoord


class Command(BaseCommand):
    help = "Loads data from Parquet file."

    # def add_arguments(self, parser):
    #     parser.add_argument("file_path", type=str, help='path for parquet file')

    @staticmethod
    def get_fields():  # add img_id to identify images in load_data
        fields = ['name', 'RA', 'DEC', 'ztf_name', 'comment', 'source_class', 'master_source', 'dup_id', 'L', 'B',
                  'R98', 'FLAG', 'qual', 'g_d2d', 'g_s', 'g_id', 's_d2d', 's_id', 's_z', 's_otype', 's_nsrc', 'checked',
                  'flag_xray', 'flag_radio', 'flag_agn_wise', 'w1', 'w2', 'w3', 'w1snr', 'w2snr', 'w3snr',
                  'g_nsrc', 'sdss_nsrc', 'sdss_p', 'sdss_id', 'sdss_sp', 'sdss_d2d', 'added', '_15R98', 'g_gmag',
                  'g_maxLx', 'w_nsrc', 'R98c', 'z', 'CTS_e1', 'CTS_e2', 'CTS_e3', 'CTS_e4', 'CTS_e123',
                  'D2D_e1', 'D2D_e3e2', 'D2D_e4', 'D2D_e123',
                  'EXP_e1', 'EXP_e12', 'EXP_e2', 'EXP_e3', 'EXP_e4', 'EXP_e123',
                  'FLUX_e1', 'FLUX_e2', 'FLUX_e3', 'FLUX_e4', 'FLUX_e123',
                  'G_L_e1', 'G_L_e2', 'G_L_e3', 'G_U_e1', 'G_U_e2', 'G_U_e3', 'G_e1', 'G_e2', 'G_e3',
                  'ID_e1', 'ID_e2', 'ID_e3', 'ID_e4', 'ID_e123',
                  'LIKE_e1', 'LIKE_e12', 'LIKE_e2', 'LIKE_e3', 'LIKE_e4', 'LIKE_e123',
                  'NH_L_e1', 'NH_L_e2', 'NH_L_e3', 'NH_U_e1', 'NH_U_e2', 'NH_U_e3', 'NH_e1', 'NH_e2', 'NH_e3',
                  'RADEC_ERR_e1', 'RADEC_ERR_e12', 'RADEC_ERR_e2', 'RADEC_ERR_e3', 'RADEC_ERR_e4', 'RADEC_ERR_e123',
                  'Tin_L_e1', 'Tin_L_e2', 'Tin_L_e3', 'Tin_U_e1', 'Tin_U_e2', 'Tin_U_e3', 'Tin_e1', 'Tin_e2', 'Tin_e3',
                  'UPLIM_e1', 'UPLIM_e2', 'UPLIM_e3', 'UPLIM_e4', 'UPLIM_e12', 'UPLIM_e123', 'RATIO_e3e2',
                  'TSTART_e1', 'TSTART_e2', 'TSTART_e3', 'TSTART_e4', 'TSTOP_e1', 'TSTOP_e2', 'TSTOP_e3', 'TSTOP_e4',
                  'g_b', 'ps_p', 'survey', 'file', 'row_num']
        return fields

    @staticmethod
    def change_dup_id(new_dup_id, close_dup_ids, sources):
        if close_dup_ids.size == 0:
            print('\n There is no close sources with dup_id <> {}'.format(new_dup_id))
        else:
            print('\n Sources with dup_id: {} will have new dup_id: {}'.format(close_dup_ids, new_dup_id))
            for dup_id in close_dup_ids:
                # update source's dup_id
                Source.objects.filter(dup_id=dup_id).update(dup_id=new_dup_id, master_source=False)

    @staticmethod
    def calculate_dup_id(source):
        # take new source and find nearest sources by RA DEC
        # return dup_id of the nearest source
        # dup_id values start with 1 => 0 <=> no near source found
        res_dup_id = 0

        # TODO: think about UPDATE in change_dup_id(...), using all() to refresh queryset
        sources = Source.objects.all().exclude(name=source.name)
        # print('Source: {} RA: {} DEC: {}\n'.format(source.name, source.RA, source.DEC))
        sources_ra = sources.values_list("RA", flat=True)
        sources_dec = sources.values_list("DEC", flat=True)
        sources_dup_id = np.array(list(sources.values_list("dup_id", flat=True)))
        # print('Dup_id:\n', sources_dup_id)

        # sources to np.array to use indices
        sources = np.array(list(sources))

        if source and np.all(sources):
            c1 = SkyCoord(source.RA * u.deg, source.DEC * u.deg, frame='icrs')
            c2 = SkyCoord(sources_ra * u.deg, sources_dec * u.deg, frame='icrs')
            sep = c1.separation(c2)
            # print('Separations in arcsec', sep.arcsec)

            # take sources closer then 30 arcsec
            sep_ind = np.where(sep < 30*u.arcsec)
            # sep_ind = np.where(sep < 0.1*u.deg)
            # print(sep_ind)
            if sep_ind[0].size > 0:
                sep_arcsec = sep.arcsec[sep_ind]
                sep_deg = sep.deg[sep_ind]
                nearest_sources = sources[sep_ind]
                # print close sources and distance in arcseconds
                for s, d, d1 in zip(nearest_sources[np.argsort(sep_arcsec)], np.sort(sep_arcsec), np.sort(sep_deg)):
                    print('\n Close source: {} RA: {} DEC: {} dup_id: {}'.format(s.name, s.RA, s.DEC, s.dup_id))
                    print('Separation in arcsec - {} '.format(d))
                    print('Separation in deg - {}'.format(d1))

                nearest_dup_ids = np.unique(sources_dup_id[sep_ind])
                print("Dup_id of the close sources: ", nearest_dup_ids)
                # take close source with min dup_id
                res_dup_id = np.min(nearest_dup_ids)
                print('Found close source with dup_id: ', res_dup_id)
                # Change dup_id of close sources to res_sup_id
                Command.change_dup_id(res_dup_id, nearest_dup_ids[nearest_dup_ids > res_dup_id], sources)

        return res_dup_id

    def handle(self, *args, **options):
        start_time = timezone.now()
        # file_path = options["file_path"]
        file_path = os.path.join(settings.PAVEL_DIR, 'master_xray_sources.parquet')
        # Field list in the order in which the columns should be in the table
        field_list = Command.get_fields()

        # data = pd.read_parquet(file_path, engine='fastparquet')
        table = pq.read_table(file_path)
        data = table.to_pandas()
        # replace all nan with None
        data = data.replace({np.nan: None})

        # Make Survey dirs for image data
        for i in range(1, 10):
            # check path
            print(os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i)))
            if not os.path.exists(os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i))):
                os.makedirs(os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i)))

        self.stdout.write(f'Start reading data')
        # sources = []
        # print(data)

        for row in data.itertuples():
            meta_data, m_created = MetaSource.objects.get_or_create(file_name=row.file)
            if m_created:
                self.stdout.write(f'Create new meta source for file: {row.file}')

            try:
                survey = Survey.objects.get(name=row.survey)
            except Survey.DoesNotExist:
                raise CommandError(f'Survey{row.survey} not found')

            self.stdout.write(f'\nSource name: {row.name} RA: {row.RA} DEC: {row.DEC}, survey: {row.survey}')
            # TODO: think about unique sources
            source, created = Source.objects.get_or_create(name=row.name, survey=survey,
                                                           defaults={'RA': row.RA, 'DEC': row.DEC,
                                                                     'meta_data': meta_data, 'row_num': row.row_num})
            if created:
                self.stdout.write(f'Create new source with name:{row.name}, RA: {row.RA}, '
                                  f'DEC: {row.DEC}, file:{row.file}')

            # Check that it is new source or new file
            if m_created or created:

                try:
                    self.stdout.write(f'Start filling fields...\n')
                    for i, field in enumerate(field_list):
                        # self.stdout.write(f'Num:{i} - {field} - {row[i+2]}')  # i+2 - skip index and img_id
                        if field not in ['name', 'RA', 'DEC', 'survey', 'file', 'row_num']:
                            setattr(source, field, row[i+2])  # Similar to source.field = row[i+1]

                    # Calculate dup_id
                    ex_dup_id = Command.calculate_dup_id(source)
                    if ex_dup_id > 0:
                        source.dup_id = ex_dup_id
                        source.master_source = False
                    else:
                        max_dup_id = Survey.get_max_dup_id()
                        source.dup_id = max_dup_id + 1
                        print('Create new dup_id: ', max_dup_id + 1)

                    # sources.append(source)
                    source.save()

                except Exception as e:
                    # Delete created source if there was ERROR while filling fields
                    if created: source.delete()
                    raise CommandError(e)

            # Rename and Copy images to static/images
            # images_path = os.path.join(settings.PAVEL_DIR, 'pavel_images')
            images_path = 'C:/Users/fedor/Desktop/4 курс/Diploma/Научная работа/sources_Xray_data/xray_master_data/'
            for i in range(1, 10):
                # Copy Light Curve
                old_path = os.path.join(images_path, 'lc_' + str(row.img_id) + '_e' + str(i) + '.pdf')
                if os.path.isfile(old_path):
                    new_file_name = 'lc_' + row.file + str(row.row_num) + '.pdf'
                    new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                    shutil.copy(old_path, new_path)

                # Copy Spectrum
                # old_path = os.path.join(images_path, 'spec_' + str(source.img_id) + '_e' + str(i) + '.pdf')
                old_path = os.path.join(images_path, 'src_'+str(row.img_id)+'_020_SourceSpec_00001'+'_e' + str(i)+'_rbkg_5_diskbb.pdf')
                if os.path.isfile(old_path):
                    new_file_name = 'spec_' + row.file + str(row.row_num) + '.pdf'
                    new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                    shutil.copy(old_path, new_path)

                # Copy Trans Image
                old_path = os.path.join(images_path, 'trans_' + str(row.img_id) + '_e' + str(i) + '.png')
                if os.path.isfile(old_path):
                    new_file_name = 'trans_' + row.file + str(row.row_num) + '.png'
                    new_path = os.path.join(settings.IMAGE_DATA_PATH, 'e' + str(i), new_file_name)
                    shutil.copy(old_path, new_path)

            # if len(sources) > 500:
            #     Source.objects.bulk_create(sources)
            #     sources = []
            #

        # if sources:
        #     Source.objects.bulk_create(sources)

        self.stdout.write(f'End reading table')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Loading Parquet took: {(end_time-start_time).total_seconds()} seconds.'))
