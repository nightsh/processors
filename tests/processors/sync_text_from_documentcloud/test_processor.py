# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import mock
import uuid
import pytest
import processors.sync_text_from_documentcloud.processor as processor


class TestSyncTextFromDocumentCloud(object):

    @mock.patch('processors.base.writers.write_file')
    @mock.patch('documentcloud.DocumentCloud')
    def test_updates_files_with_documentcloud_text(self, dc_mock, write_file_mock):
        conf = {
            'DOCUMENTCLOUD_USERNAME': 'username',
            'DOCUMENTCLOUD_PASSWORD': 'password',
        }
        conn = {
            'database': mock.Mock(),
        }
        the_file = {
            'id': uuid.uuid1(),
            'documentcloud_id': '100-foo',
        }
        conn['database'].query.return_value = [the_file]
        _enable_documentcloud_mock(dc_mock)
        dc_mock().documents.get().get_full_text.return_value = 'full text'

        processor.process(conf, conn)

        dc_mock().documents.get.assert_called_with(the_file['documentcloud_id'])
        write_file_mock.assert_called_with(conn, {
            'id': the_file['id'].hex,
            'text': 'full text'
        })

    @mock.patch('documentcloud.DocumentCloud')
    def test_ignores_documents_without_fulltext(self, dc_mock):
        conf = {
            'DOCUMENTCLOUD_USERNAME': 'username',
            'DOCUMENTCLOUD_PASSWORD': 'password',
        }
        conn = {
            'database': mock.Mock(),
        }
        conn['database'].query.return_value = [
            {'id': 'file_id', 'documentcloud_id': '100-foo'},
        ]
        dc_mock().documents.get().get_full_text.side_effect = NotImplementedError()

        processor.process(conf, conn)

        conn['database'].update.assert_not_called()

    @mock.patch('documentcloud.DocumentCloud')
    def test_ignores_documents_it_couldnt_load(self, dc_mock):
        conf = {
            'DOCUMENTCLOUD_USERNAME': 'username',
            'DOCUMENTCLOUD_PASSWORD': 'password',
        }
        conn = {
            'database': mock.Mock(),
        }
        conn['database'].query.return_value = [
            {'id': 'file_id', 'documentcloud_id': '100-foo'},
        ]
        dc_mock().documents.get.side_effect = Exception()

        processor.process(conf, conn)

    @mock.patch('documentcloud.DocumentCloud')
    def test_raises_stuff(self, dc_mock):
        conf = {
            'DOCUMENTCLOUD_USERNAME': 'username',
            'DOCUMENTCLOUD_PASSWORD': 'password',
        }
        conn = {
            'database': mock.Mock(),
        }
        conn['database'].query.return_value = [
            {'id': 'file_id', 'documentcloud_id': '100-foo'},
        ]
        exception = Exception()
        exception.code = 403
        dc_mock().documents.get.side_effect = exception

        with pytest.raises(Exception):
            processor.process(conf, conn)


def _enable_documentcloud_mock(dc_mock):
    project = mock.Mock()

    document = mock.Mock()

    client = mock.Mock()
    client.projects.get_by_title.return_value = project
    client.documents.get.return_value = document

    dc_mock.return_value = client

    return dc_mock
