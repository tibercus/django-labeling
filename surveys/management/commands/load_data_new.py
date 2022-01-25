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
    def change_meta_object(new_meta_obj, close_meta_objs):
        if len(close_meta_objs) == 0:
            print('\n There is no close sources with meta object <> {}'.format(new_meta_obj))
        else:
            print('\n Sources with meta objs: {} will have new meta obj: {}'.format(close_meta_objs, new_meta_obj))
            for meta_obj in close_meta_objs:
                # update source's meta objects
                Source.objects.filter(meta_object__master_name=meta_obj).update(meta_object=new_meta_obj, master_source=False)
                # delete meta objects with no sources
                MetaObject.objects.filter(master_name=meta_obj).delete()

    @staticmethod
    def find_or_create_mobject(s_name, s_ra, s_dec):
        # take source and find nearest meta_object by RA DEC
        # return nearest meta_object or create new

        sources = Source.objects.all().exclude(name=s_name)
        print('\nMeta object for source: {} RA: {} DEC: {}\n'.format(s_name, s_ra, s_dec))
        sources_ra = sources.values_list('RA', flat=True)
        sources_dec = sources.values_list('DEC', flat=True)
        sources_meta = np.array(list(sources.values_list('meta_object__master_name', flat=True)))
        # print('Meta objects: ', sources_meta)

        # sources to np.array to use indices
        np_sources = np.array(list(sources))

        if np.all(np_sources):
            c1 = SkyCoord(s_ra * u.deg, s_dec * u.deg, frame='icrs')
            c2 = SkyCoord(sources_ra * u.deg, sources_dec * u.deg, frame='icrs')
            sep = c1.separation(c2)
            # print('Separations in arcsec', sep.arcsec)

            # take sources closer then 30 arcsec
            sep_ind = np.where(sep < 30 * u.arcsec)
            # print('Indices: ', sep_ind)
            if sep_ind[0].size > 0:
                sep_arcsec = sep.arcsec[sep_ind]
                sep_deg = sep.deg[sep_ind]
                nearest_sources = np_sources[sep_ind]
                # print close sources and distance in arcseconds
                for s, a, d in zip(nearest_sources[np.argsort(sep_arcsec)], np.sort(sep_arcsec), np.sort(sep_deg)):
                    print('\n Close source: {} RA: {} DEC: {} meta_obj: {}'.format(s.name, s.RA, s.DEC, s.meta_object))
                    print('Separation in arcsec - {} '.format(a))
                    print('Separation in deg - {}'.format(d))

                meta_of_nearest = list(np.unique(sources_meta[sep_ind]))
                print("Meta_objects names of the close sources: ", meta_of_nearest)
                meta_obj = nearest_sources[np.argsort(sep_arcsec)][0].meta_object
                print("Take meta object: ", meta_obj)
                created = False
                # change meta objects of close sources
                meta_of_nearest.remove(meta_obj.master_name)
                Command.change_meta_object(meta_obj, meta_of_nearest)

            else:
                meta_obj = MetaObject.objects.create(master_name=s_name)
                print('Created new meta object: {}\n'.format(meta_obj.master_name))
                created = True

        return meta_obj, created

    def handle(self, *args, **options):
        start_time = timezone.now()
        # file_path = options["file_path"]
        file_path = os.path.join(settings.WORK_DIR, 'master_xray_sources.parquet')
        # file_path = os.path.join(settings.WORK_DIR, 'test_dup_id.parquet')

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
            # find/create meta source - file
            meta_data, m_created = MetaSource.objects.get_or_create(file_name=row.file)
            if m_created:
                self.stdout.write(f'Create new meta source for file: {row.file}')

            # find source's survey
            try:
                survey = Survey.objects.get(name=row.survey)
            except Survey.DoesNotExist:
                raise CommandError(f'Survey{row.survey} not found')

            # find/create source and find/create meta_object
            try:  # look for existing source
                # TODO: think about unique sources
                source = Source.objects.get(name=row.name, survey=survey, meta_data=meta_data, row_num=row.row_num)
                created = False
                self.stdout.write(f'\nSource name: {row.name} RA: {row.RA} DEC: {row.DEC}, survey: {row.survey}\n')

            except Source.DoesNotExist:
                meta_object, o_created = Command.find_or_create_mobject(row.name, row.RA, row.DEC)
                print('Result - {} {}'.format(meta_object, o_created))
                self.stdout.write(f'\n New source name: {row.name} RA: {row.RA} DEC: {row.DEC}, survey: {row.survey}\n')
                # create new source
                source = Source.objects.create(name=row.name, survey=survey, RA=row.RA, DEC=row.DEC,
                                               meta_data=meta_data, row_num=row.row_num,
                                               meta_object=meta_object, master_source=o_created)
                created = True

            if created:
                self.stdout.write(f'Created new source with name:{row.name}, RA: {row.RA}, '
                                  f'DEC: {row.DEC}, file:{row.file}')

            # Check that it is new source or new file
            if m_created or created:

                try:
                    self.stdout.write(f'Start filling fields...\n')
                    for i, field in enumerate(field_list):
                        # self.stdout.write(f'Num:{i} - {field} - {row[i+2]}')  # i+2 - skip index and img_id
                        filled_fields = ['name', 'RA', 'DEC', 'survey', 'file', 'row_num', 'master_source']
                        if field not in filled_fields:
                            setattr(source, field, row[i+2])  # Similar to source.field = row[i+1]

                    # sources.append(source)
                    source.save()

                except Exception as e:
                    # Delete created source if there was ERROR while filling fields
                    if created: source.delete()
                    raise CommandError(e)

            # Rename and Copy images to static/images
            images_path = settings.MASTER_DIR
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

            # maybe use this later
            # if len(sources) > 500:
            #     Source.objects.bulk_create(sources)
            #     sources = []
            #

        # if sources:
        #     Source.objects.bulk_create(sources)

        self.stdout.write(f'End reading table')
        end_time = timezone.now()
        self.stdout.write(self.style.SUCCESS(f'Loading Parquet took: {(end_time-start_time).total_seconds()} seconds.'))
