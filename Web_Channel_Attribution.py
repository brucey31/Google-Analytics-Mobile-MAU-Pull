#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simple intro to using the Google Analytics API v3.
This application demonstrates how to use the python client library to access
Google Analytics data. The sample traverses the Management API to obtain the
authorized user's first profile ID. Then the sample uses this ID to
contstruct a Core Reporting API query to return the top 25 organic search
terms.
Before you begin, you must sigup for a new project in the Google APIs console:
https://code.google.com/apis/console
Then register the project to use OAuth2.0 for installed applications.
Finally you will need to add the client id, client secret, and redirect URL
into the client_secrets.json file that is in the same directory as this sample.
Sample Usage:
  $ python hello_analytics_api_v3.py
Also you can also get help on all the command-line flags the program
understands by running:
  $ python hello_analytics_api_v3.py --help
"""
from __future__ import print_function

import argparse
import sys

from googleapiclient.errors import HttpError
from googleapiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
from datetime import date, timedelta, datetime
import csv
import json
from subprocess import call, check_output
import configparser
import psycopg2
import os

__author__ = 'api.nickm@gmail.com (Nick Mihailovski)'

import argparse
import sys

from googleapiclient.errors import HttpError
from googleapiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError


config = configparser.ConfigParser()
ini = config.read('conf2.ini')

AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')


def main(argv):
  # Authenticate and construct service.
  service, flags = sample_tools.init(
      argv, 'analytics', 'v3', __doc__, __file__,
      scope='https://www.googleapis.com/auth/analytics.readonly')

  # Try to make a request to the API. Print the results or handle errors.
  try:
    first_profile_id = get_first_profile_id(service)
    if not first_profile_id:
      print('Could not find a valid profile for this user.')

    else:

      start_date = date(2016, 5, 1)
      end_date = date(2016, 5, 2)

      while end_date <= date.today():
        rs = check_output(["s3cmd", "ls", "s3://bibusuu/Web_Acquisition_Channel/%s" % start_date])

        if len(rs) > 1:
            print("File Exists for %s, Skipping processing for this file" % start_date)

        else:
            page_index = 1

            results = get_top_keywords(service, first_profile_id, start_date, end_date, page_index)
            max_pages = results['totalResults']

            while page_index <= max_pages:
                print("Grabbing Acquisition data for %s to %s page %s" % (start_date, end_date, page_index))
                results = get_top_keywords(service, first_profile_id, start_date, end_date, page_index)
                print_results(results, start_date, page_index)

                page_index = page_index + 10000

        start_date = start_date + timedelta(days=1)
        end_date = end_date + timedelta(days=1)

  except TypeError as error:
    # Handle errors in constructing a query.
    print(('There was an error in constructing your query : %s' % error))

  except HttpError as error:
    # Handle API errors.
    print(('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason())))

  except AccessTokenRefreshError:
    # Handle Auth errors.
    print ('The credentials have been revoked or expired, please re-run '
           'the application to re-authorize')


def get_first_profile_id(service):
  """Traverses Management API to return the first profile id.
  This first queries the Accounts collection to get the first account ID.
  This ID is used to query the Webproperties collection to retrieve the first
  webproperty ID. And both account and webproperty IDs are used to query the
  Profile collection to get the first profile id.
  Args:
    service: The service object built by the Google API Python client library.
  Returns:
    A string with the first profile ID. None if a user does not have any
    accounts, webproperties, or profiles.
  """

  accounts = service.management().accounts().list().execute()

  if accounts.get('items'):
    firstAccountId = accounts.get('items')[0].get('id')
    webproperties = service.management().webproperties().list(
        accountId=firstAccountId).execute()

    if webproperties.get('items'):
      firstWebpropertyId = webproperties.get('items')[0].get('id')
      profiles = service.management().profiles().list(
          accountId=firstAccountId,
          webPropertyId=firstWebpropertyId).execute()

      if profiles.get('items'):
        return profiles.get('items')[0].get('id')

  return None


def get_top_keywords(service, profile_id, start_date, end_date, page_index):

  """Executes and returns data from the Core Reporting API.
  This queries the API for the top 25 organic search terms by visits.
  Args:
    service: The service object built by the Google API Python client library.
    profile_id: String The profile ID from which to retrieve analytics data.
    start_date: the dat to start the report from.
    end_date: The day to end the report from
    page_index: Get even more results
  Returns:
    The response returned from the Core Reporting API.
  """

  return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date='%s' % start_date,
      end_date='%s' % end_date,
      metrics='ga:users',
      dimensions='ga:date,ga:channelGrouping,ga:sourceMedium,ga:campaign,ga:socialNetwork,ga:keyword,ga:dimension2',
      #sort='-ga:visits',
      filters='ga:channelGrouping!=Direct',
      start_index='%s' % page_index,
      max_results='1000000').execute()


def print_results(results, start_date, page_index):
  """Prints out the results.
  This prints out the profile name, the column headers, and all the rows of
  data.
  Args:
    results: The response returned from the Core Reporting API.
    start_date: the date to put on the fullname when it is written down
    page_index: add more stuff to the day
  """

  # print('Profile Name: %s' % results.get('profileInfo').get('profileName'))

  # Print header.
  if results.get('rows', []):
      with open('Web_Channel_Attribution_%s_%s.csv' % (start_date, page_index), 'wb') as csvfile:

          spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|')

          for row in results.get('rows'):
              output = []
              for cell in row:
                  output.append('%s' % cell)
              spamwriter.writerow([s.encode('ascii', 'ignore') for s in output])


      print('Uploading %s page %s to S3' % (start_date, page_index))
      call(["s3cmd", "put", 'Web_Channel_Attribution_%s_%s.csv' % (start_date, page_index), "s3://bibusuu/Web_Acquisition_Channel/%s/Web_Channel_Attribution_%s_%s.csv" % (start_date, start_date, page_index)])
      os.remove('Web_Channel_Attribution_%s_%s.csv' % (start_date, page_index))

      conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (
      RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
      print("Connecting to database\n        ->%s" % (conn_string))
      conn = psycopg2.connect(conn_string)

      cursor = conn.cursor()
      # Update the redshift table with the new results

      print("Deleting old table web_acquisition_channel2")
      cursor.execute("drop table if exists web_acquisition_channel2;")
      print("Creating new table \n web_acquisition_channel2 ")
      cursor.execute("")
      print("Copying Web Acquisition Channel data from S3 to  \n web_acquisition_channel2 ")
      cursor.execute("COPY web_acquisition_channel2  FROM 's3://bibusuu/Web_Acquisition_Channel/'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))

      print("Dropping Table  \n web_acquisition_channel")
      cursor.execute("DROP TABLE if exists web_acquisition_channel;")
      print("Renaming web_acquisition_channel2 to\n web_acquisition_channel ")
      cursor.execute("ALTER TABLE web_acquisition_channel2 rename to web_acquisition_channel")

      conn.commit()
      conn.close()



  else:
    print('No Rows Found')


if __name__ == '__main__':
  main(sys.argv)