# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

# import json
import datetime
from copy import deepcopy
# from .. import base
import logging
logger = logging.getLogger(__name__)


# Module API

def process(conf, conn):
    """Merge trial identifiers
    """

    timestamp = datetime.datetime.utcnow()

    # Prepare
    query = """
        SELECT trials.id  as trial_id,   trials.identifiers as trial_identifiers, trials.source_id,
               records.id as record_id, records.identifiers as record_identifiers
        FROM trials
        INNER JOIN records ON records.trial_id = trials.id
    """

    # Execute
    count = 0
    failed = 0

    for result in conn['database'].query(query):

        record_identifiers = result['record_identifiers'] or {}
        trial_identifiers = result['trial_identifiers'] or {}
        new_identifiers = deepcopy(trial_identifiers)

        diff = set(record_identifiers.keys()) - set(trial_identifiers.keys())

        if len(diff) == 0:
            continue

        # Get trial by ID, since it's much faster than using the helper
        obj = conn['database']['trials'].find_one(id=result['trial_id'].hex)

        if not obj:
            logger.warn('Could not get trial with ID {}'.format(result['trial_id']))
            failed += 1
            continue

        count += 1

        for value in diff:
            new_identifiers[value] = record_identifiers[value]

        # Update meta
        obj.update({
            'updated_at': timestamp,
            'identifiers': new_identifiers,
        })

        try:
            conn['database']['trials'].update(obj, ['id'], ensure=False)
            logger.info("[{}] Trial {} was updated by record {}".format(count, result['trial_id'], result['record_id']))
        except Exception as e:
            failed += 1
            logger.warn("[ !! {}] Trial {} could not be updated".format(count, result['trial_id']))
            logger.debug(e)

    logger.info('{} trials updated'.format(count - failed))
    logger.info('{} trials failed'.format(failed))
