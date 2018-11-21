# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('eventsmanager', '0015_event_derived_from_id'),
    ]

    operations = [
        migrations.AddField(
            model_name='event',
            name='organization',
            field=models.CharField(blank=True, default='', max_length=800),
            preserve_default=True,
        ),
        migrations.AlterField(
            model_name='event',
            name='creator_path',
            field=models.CharField(default='CEDUS', max_length=1024),
        ),
    ]
