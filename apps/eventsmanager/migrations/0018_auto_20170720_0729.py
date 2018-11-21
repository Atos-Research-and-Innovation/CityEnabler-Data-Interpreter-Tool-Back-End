# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('eventsmanager', '0017_auto_20170720_0726'),
    ]

    operations = [
        migrations.AlterField(
            model_name='event',
            name='languageID',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
