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
        task = {}
        task['documents'] = []

        for doc in group:
            task['supplement_number'] = str(doc['supplement_number'])
            task['approval_type'] = str(doc['type'])
            task['approval_id'] = str(doc['id'])
            task['meta_id'] = str(doc['meta_id'])
            task['notes'] = doc['notes'] or ""
            task['documents'].append({
                'name': doc['name'],
                'url': doc['url']
            })
        tasks.append(task)
    logger.debug('{} tasks in the database'.format(len(tasks)))

    _submit_tasks(tasks)


def _pybossa_rate_limitation(endpoint):
    # This should be called before actual requests to avoid getting HTTP 429s
    res = requests.get('{}/api/{}'.format(PYBOSSA_URL, endpoint))
    if int(res.headers['X-RateLimit-Remaining']) < 30:
        logger.warn('Rate limit reached, will sleep for 5 minutes')
        time.sleep(300)  # Sleep for 5 minutes


def _get_existing_ids(PROJECT_ID):
    existing_ids = []
    tasks = []
    offset = 0
    limit = 100

    # Make sure we have enough requests left
    _pybossa_rate_limitation('task')
    while True:
        collection = pbc.get_tasks(PROJECT_ID, limit=limit, offset=offset)
        if len(collection) > 0:
            tasks.extend(collection)
            offset += limit
        else:
            break
    for task in tasks:
        existing_ids.append(task.info['meta_id'])
    return existing_ids


def _submit_tasks(tasks):

    # Set up PyBossa connection
    pbc.set('endpoint', PYBOSSA_URL)
    pbc.set('api_key', PYBOSSA_API_KEY)

    # Get existing IDs
    existing_ids = _get_existing_ids(PROJECT_ID)
    if not isinstance(existing_ids, list):
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
