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

  $ python Android_MAU.py

Also you can also get help on all the command-line flags the program
understands by running:

  $ python Android_MAU.py --help
"""
from __future__ import print_function

__author__ = 'api.nickm@gmail.com (Nick Mihailovski)'

import argparse
import sys

from googleapiclient.errors import HttpError
from googleapiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
import datetime
import csv
from subprocess import call
import configparser
import psycopg2
import os

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
      results = get_top_keywords(service, first_profile_id)
      print_results(results)

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
    webproperties = service.management().webproperties().list(accountId=firstAccountId).execute()

    if webproperties.get('items'):
      firstWebpropertyId = webproperties.get('items')[10].get('id')
      profiles = service.management().profiles().list(
          accountId=firstAccountId,
          webPropertyId=firstWebpropertyId).execute()

      if profiles.get('items'):
        return profiles.get('items')[1].get('id')

  return None


def get_top_keywords(service, profile_id):
  """Executes and returns data from the Core Reporting API.

  This queries the API for the top 25 organic search terms by visits.

  Args:
    service: The service object built by the Google API Python client library.
    profile_id: String The profile ID from which to retrieve analytics data.

  Returns:
    The response returned from the Core Reporting API.
  """

  return service.data().ga().get(
      ids='ga:' + profile_id,
      start_date='2016-01-12',
      end_date='%s' % datetime.date.today(),
      metrics='ga:30dayUsers',
      dimensions='ga:date').execute()


def print_results(results):
  """Prints out the results.

  This prints out the profile name, the column headers, and all the rows of
  data.

  Args:
    results: The response returned from the Core Reporting API.
  """

  # print('Profile Name: %s' % results.get('profileInfo').get('profileName'))


  # Print header.
  output = []
  for header in results.get('columnHeaders'):
    output.append('%30s' % header.get('name'))
  # print(''.join(output))

  # Print data table.
  if results.get('rows', []):
    with open('Android_MAU.csv', 'wb') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in results.get('rows'):
          row.append("Android")
          spamwriter.writerow(row)
          output = []
          for cell in row:
            output.append('%s' % cell)

            # print(',Android,'.join(output))
    print('Uploading to Android MAU to S3')
    call(["s3cmd", "put", 'Android_MAU.csv', "s3://bibusuu/Android_MAU/"])
    os.remove('Android_MAU.csv')

    conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
    print("Connecting to database\n        ->%s" % (conn_string))
    conn = psycopg2.connect(conn_string)

    cursor = conn.cursor()
    # Update the redshift table with the new results
    print("Deleting old table Android_MAU2")
    cursor.execute("drop table if exists Android_MAU2;")
    print("Creating new table \n Android_MAU2 ")
    cursor.execute( "create table Android_MAU2(date varchar(10), MAU int, Platform varchar(20));")
    print("Copying Android_MAU data from S3 to  \n Android_MAU2 ")
    cursor.execute( "COPY Android_MAU2  FROM 's3://bibusuu/Android_MAU/'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))



    print("Dropping Table  \n Android_MAU ")
    cursor.execute("DROP TABLE if exists Android_MAU;")
    cursor.execute("create table Android_mau as select concat(concat(concat(concat(substring(date,0,5),'-'),substring(date,5,2)),'-'),substring(date,7,2))::DATE as date, mau, platform from Android_mau2;")
    print("Dropping table Android_mau2 ")
    cursor.execute("DROP TABLE if exists Android_MAU2")

    conn.commit()
    conn.close()



  else:
    print('No Rows Found')


if __name__ == '__main__':
  main(sys.argv)
