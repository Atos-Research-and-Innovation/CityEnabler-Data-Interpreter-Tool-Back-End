# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datasetmanager', '0014_auto_20170720_0757'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='resource_issued',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='resource_publisher',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AlterField(
            model_name='dataset',
            name='resource_url',
            field=models.URLField(blank=True, max_length=500, null=True),
        ),
    ]
