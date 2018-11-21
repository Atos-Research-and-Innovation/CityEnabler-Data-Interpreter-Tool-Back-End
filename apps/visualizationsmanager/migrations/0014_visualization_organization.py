# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('visualizationsmanager', '0013_auto_20160916_0657'),
    ]

    operations = [
        migrations.AddField(
            model_name='visualization',
            name='organization',
            field=models.CharField(max_length=800, default='', blank=True),
            preserve_default=True,
        ),
    ]
