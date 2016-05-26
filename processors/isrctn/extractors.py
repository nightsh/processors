# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
from .. import base


# Module API

def extract_source(record):
    source = {
        'id': 'isrctn',
        'name': 'ISRCTN',
        'type': 'register',
    }
    return source


def extract_trial(record):

    # Get identifiers
    identifiers = base.helpers.clean_dict({
        'nct': record['clinicaltrials_gov_number'],
        'isrctn': record['isrctn_id'],
    })

    # Get public title
    public_title = base.helpers.get_optimal_title(
        record['title'],
        record['scientific_title'],
        record['isrctn_id'])

    # Get recruitment status
    statuses = {
        'No longer recruiting': 'complete',
        'Not yet recruiting': 'pending',
        'Recruiting': 'recruiting',
        'Stopped': 'complete',
    }
    recruitment_status = statuses[record['recruitment_status']]

    # Get target sample size
    try:
        target_sample_size = int(record['target_number_of_participants'])
    except Exception:
        target_sample_size = None

    # Get gender
    gender = None
    if record['gender'] and record['gender'] != 'Not Specified':
        gender = record['gender'].lower()

    # Get has_published_results
    has_published_results = False
    if record['results_basic_reporting']:
        has_published_results = True

    trial = {
        'primary_register': 'ISRCTN',
        'primary_id': record['isrctn_id'],
        'identifiers': identifiers,
        'registration_date': record['date_applied'],
        'public_title': public_title,
        'brief_summary': record['plain_english_summary'],
        'scientific_title': record['scientific_title'],
        'description': record['plain_english_summary'],
        'recruitment_status': recruitment_status,
        'eligibility_criteria': {
            'inclusion': record['participant_inclusion_criteria'],
            'exclusion': record['participant_exclusion_criteria'],
        },
        'target_sample_size': target_sample_size,
        'first_enrollment_date': record['overall_trial_start_date'],
        'study_type': record['primary_study_design'],
        'study_design': record['study_design'],
        'study_phase': record['phase'],
        'primary_outcomes': record['primary_outcome_measures'],
        'secondary_outcomes': record['secondary_outcome_measures'],
        'gender': gender,
        'has_published_results': has_published_results,
    }
    return trial


def extract_conditions(record):
    conditions = []
    name = base.helpers.clean_string(record['condition'])
    if name:
        conditions.append({
            'name': name,
        })
    return conditions


def extract_interventions(record):
    interventions = []
    if record['drug_names']:
        pattern = r'(?:,)|(?:\d+\.)'
        for element in re.split(pattern, record['drug_names']):
            name = base.helpers.clean_string(element)
            if name:
                interventions.append({
                    'name': name,
                    'type': 'drug',
                })
    return interventions


def extract_locations(record):
    locations = []
    for element in (record['countries_of_recruitment'] or '').split(',') or []:
        name = base.helpers.clean_string(element)
        if name:
            locations.append({
                'name': name,
                'type': 'country',
                # ---
                'trial_role': 'recruitment_countries',
            })
    return locations


def extract_organisations(record):
    organisations = []
    for element in record['sponsors'] or []:
        name = base.helpers.clean_string(element.get('organisation', ''))
        if name:
            organisations.append({
                'name': name,
                # ---
                'trial_role': 'sponsor',
            })
    for element in record['funders'] or []:
        name = base.helpers.clean_string(element.get('funder_name', ''))
        if name:
            organisations.append({
                'name': name,
                # ---
                'trial_role': 'funder',
            })
    return organisations


def extract_persons(record):
    persons = []
    for element in record['contacts'] or []:
        name = base.helpers.clean_string(
            element.get('primary_contact', element.get('additional_contact')))
        if name:
            persons.append({
                'name': name,
                # ---
                'trial_id': record['isrctn_id'],
            })
    return persons
