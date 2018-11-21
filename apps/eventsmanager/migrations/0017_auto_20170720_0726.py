# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('eventsmanager', '0016_auto_20170517_1708'),
    ]

    operations = [
        migrations.AlterField(
            model_name='event',
            name='languageID',
            field=models.IntegerField(null=True),
        ),
    ]
