# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('visualizationsmanager', '0015_auto_20170519_1104'),
    ]

    operations = [
        migrations.AlterField(
            model_name='visualization',
            name='creator_path',
            field=models.CharField(max_length=1024),
        ),
    ]
