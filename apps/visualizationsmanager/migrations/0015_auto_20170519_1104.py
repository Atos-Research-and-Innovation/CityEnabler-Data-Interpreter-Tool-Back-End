# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('visualizationsmanager', '0014_visualization_organization'),
    ]

    operations = [
        migrations.AlterField(
            model_name='visualization',
            name='organization',
            field=models.CharField(blank=True, default='', null=True, max_length=800),
        ),
    ]
