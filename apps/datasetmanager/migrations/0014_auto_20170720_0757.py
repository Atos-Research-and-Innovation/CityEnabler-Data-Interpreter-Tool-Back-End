# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datasetmanager', '0013_dataset_organization'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='language_id',
            field=models.IntegerField(null=True, blank=True),
        ),
    ]
