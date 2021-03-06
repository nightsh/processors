# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import documentcloud
logger = logging.getLogger(__name__)


def process(conf, conn):
    query = (
        'SELECT documentcloud_id FROM files'
        ' WHERE documentcloud_id IS NOT NULL'
    )
    dc_local_ids = set([_extract_dc_id(row['documentcloud_id'])
                        for row in conn['database'].query(query)])
    dc_remote_ids = set([_extract_dc_id(dc_id)
                         for dc_id in _documentcloud_projects(conf)])
    dc_ids = dc_remote_ids - dc_local_ids

    dc_client = _documentcloud_client(conf)
    for dc_id in dc_ids:
        logger.debug('Deleting from DocumentCloud: %s' % dc_id)
        document = dc_client.documents.get(dc_id)
        if document:
            document.delete()


def _documentcloud_projects(conf):
    project_title = conf['DOCUMENTCLOUD_PROJECT']
    client = _documentcloud_client(conf)

    project = client.projects.get_by_title(project_title)
    return project.document_ids


def _documentcloud_client(conf):
    username = conf['DOCUMENTCLOUD_USERNAME']
    password = conf['DOCUMENTCLOUD_PASSWORD']

    return documentcloud.DocumentCloud(username, password)


def _extract_dc_id(dc_id):
    '''Return only the numeric ID part of a DocumentCloud ID.

    DocumentCloud IDs have the format "00000-document-title", but only the
    numbers are used as identifier. This means that documents with IDs
    "100-foo" and "100-bar" are actually the same.

    This function returns only the numeric part of an ID.
    '''
    parts = dc_id.split('-')
    if parts:
        return parts[0]
