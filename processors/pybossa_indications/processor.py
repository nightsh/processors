# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from itertools import groupby

import os
import time
import requests
import pbclient as pbc
import logging
logger = logging.getLogger(__name__)

PYBOSSA_URL = os.environ['PYBOSSA_URL']
PYBOSSA_API_KEY = os.environ['PYBOSSA_API_KEY']
PROJECT_ID = os.environ['PYBOSSA_PROJECT_INDICATIONS']


def process(conf, conn):

    # Get stuff from the DB
    query = """
        SELECT
            documents.id as meta_id,
            fda_approvals.id,
            documents.name,
            documents.url,
            fda_approvals.type,
            fda_approvals.notes,
            fda_approvals.supplement_number
        FROM documents
        INNER JOIN fda_approvals ON documents.fda_approval_id = fda_approvals.id
        WHERE documents.url IS NOT NULL
        AND fda_approvals.type = 'New or Modified Indication'
        GROUP BY fda_approvals.id, documents.id
        ORDER BY fda_approvals.id
    """

    rows = list(conn['database'].query(query))
    if rows:
        _create_tasks(rows)
    else:
        logger.info('No new and modified indications found for tasks creation')


def _create_tasks(rows):
    tasks = []
    for key, group in groupby(rows, lambda x: x['id']):
        group = list(group)
        first = group[0]
        print(str(first['meta_id']))
        task = {
            'supplement_number': str(first['supplement_number']),
            'approval_type': first['type'],
            'approval_id': str(first['id']),
            'meta_id': str(first['meta_id']),
            'notes': first.get('notes', '')
        }
        task['documents'] = [{'name': doc['name'], 'url': doc['url']}
                             for doc in group]
        tasks.append(task)

    logger.debug('{} tasks in the database'.format(len(tasks)))
    _submit_tasks(tasks)


def _pybossa_rate_limitation(endpoint):
    # This should be called before actual requests to avoid getting HTTP 429s
    res = requests.get('{}/api/{}'.format(PYBOSSA_URL, endpoint))
    if int(res.headers['X-RateLimit-Remaining']) < 10:
        logger.warn('Rate limit reached, will sleep for 5 minutes')
        time.sleep(300)  # Sleep for 5 minutes


def _get_existing_ids(PROJECT_ID):
    tasks = []
    limit = 100

    # Make sure we have enough requests left
    while True:
        _pybossa_rate_limitation('task')
        last_id = tasks[-1].id if tasks else None
        collection = pbc.get_tasks(PROJECT_ID, limit=limit, last_id=last_id)
        tasks.extend(collection)
        if not collection:
            break
    logger.debug('{} tasks on the server'.format(len(tasks)))
    return [task.info['meta_id'] for task in tasks]


def _submit_tasks(tasks):

    # Set up PyBossa connection
    pbc.set('endpoint', PYBOSSA_URL)
    pbc.set('api_key', PYBOSSA_API_KEY)

    # Get existing IDs
    existing_ids = _get_existing_ids(PROJECT_ID)
    if not existing_ids:
        logger.error('Cannot get the list of existing task IDs')

    cleaned_tasks = [t for t in tasks if t['meta_id'] not in existing_ids]

    logger.debug('{} tasks to be created'.format(len(cleaned_tasks)))

    for task in cleaned_tasks:
        _submit_task(task, PROJECT_ID)


def _submit_task(task, PROJECT_ID):
    res = pbc.create_task(PROJECT_ID, task)
    if isinstance(res, dict) and res['status_code'] == 429:
        _pybossa_rate_limitation('task')
        _submit_task(task, PROJECT_ID)
