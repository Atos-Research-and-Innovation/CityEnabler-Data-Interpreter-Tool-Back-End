# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datasetmanager', '0012_dataset_derived_from_id'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='organization',
            field=models.CharField(max_length=800, null=True, default='', blank=True),
            preserve_default=True,
        ),
    ]
