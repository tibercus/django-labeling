# Generated by Django 3.2.7 on 2021-10-23 10:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('surveys', '0011_delete_images'),
    ]

    operations = [
        migrations.AddField(
            model_name='comment',
            name='master_com',
            field=models.BooleanField(blank=True, default=False, null=True),
        ),
    ]
