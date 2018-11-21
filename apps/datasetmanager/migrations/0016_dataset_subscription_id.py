# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datasetmanager', '0015_auto_20170720_0822'),
    ]

    operations = [
        migrations.AddField(
            model_name='dataset',
            name='subscription_id',
            field=models.CharField(max_length=800, default='', blank=True, null=True),
            preserve_default=True,
        ),
    ]
