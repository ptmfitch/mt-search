#!/usr/bin/env python

########################################################################
# NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
#
# This is the absolute, simplest Locust example that exists.
# All it does is demonstrate is how to create a Locust task
# and utilize the 'Host' parameter in the Locust UI
# to dynamically pass in variables to the script
# so you don't need to hardcode any sensitive logic
# and offers reusability for future projects.
#
# ANYTHING THAT REQUIRES YOUR ATTENTION WILL HAVE A TODO IN THE COMMENTS
# Do not create external files outside of this locust file.
# mLocust only allows you to upload a single python file atm.
#
########################################################################

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import gevent
_ = gevent.monkey.patch_all()

########################################################################
# TODO Add any additional imports here.
# TODO Make sure to include in requirements.txt, if necessary.
########################################################################
import pymongo
from locust import User, events, task
from random import choice, choices, random
from string import digits, ascii_lowercase, ascii_uppercase
from datetime import datetime, timedelta
import time

# Global vars
# Store the client conn globally so we don't create a conn pool for every user
# Track the srv globally so we know if we need to reinit the client
_CLIENT = None
_SRV = None

class MetricsLocust(User):

    ########################################################################
    # Class variables.
    # The values are initialized with None till they get set
    # from the actual locust exeuction when the 'Host' param is passed in.
    ########################################################################
    client, coll = None, None

    ####################################################################
    # You can throttle tasks being executed by each simulated user
    # Only do this if the client really wants to simulate n-number
    # of users. Otherwise, if you leave this commented out,
    # the performance will increase by 400%
    ####################################################################
    # wait_time = between(1, 1)

    ####################################################################
    # Initialize any env vars from the Host parameter
    # Set the target collections and such here
    ####################################################################
    def __init__(self, parent):
        global _CLIENT, _SRV

        super().__init__(parent)

        try:
            # Parse out env variables from the host
            vars = self.host.split("|")
            print("Host Param:",self.host)
            srv = vars[0]
            if _SRV != srv:
                self.client = pymongo.MongoClient(srv)
                _CLIENT = self.client
                _SRV = srv
            else:
                self.client = _CLIENT

            db = self.client[vars[1]]
            self.coll = db[vars[2]]
        except Exception as e:
            # If an exception is caught, Locust will show a task with the error msg in the UI for ease
            events.request.fire(request_type="Host Init Failure", name=str(e), response_time=0, response_length=0, exception=e)
            raise e

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    # TODO Create any additional helper functions here
    ################################################################
    def get_time(self):
        return time.time()


    def random_account_no(self):
        return ''.join(choices(digits, k=12))


    def random_number(self, min, max):
        return int(random() * (max - min) + min)


    ACCOUNT_NUMBER = "Acc.No"
    ENTERED_DATE = "EntDt"

    def account_no_in(self, accounts):
        return {
            'in': {
                'path': self.ACCOUNT_NUMBER,
                'value': accounts
            }
        }


    def account_no_eq(self, account):
        return {
            'equals': {
                'path': self.ACCOUNT_NUMBER,
                'value': account
            }
        }

    def date_range(self, start, end, path=ENTERED_DATE):
        return {
            'range': {
                'path': path,
                'gt': start,
                'lt': end
            }
        }


    def date_range_days_from_today(self, days, path=ENTERED_DATE):
        return self.date_range(
            datetime.now() - timedelta(days=days),
            datetime.now(),
            path
        )


    def sort(self, primary_field, primary_direction, secondary_field=None, secondary_direction=None):
        sort = { primary_field: primary_direction }

        if secondary_field:
            sort[secondary_field] = secondary_direction

        return sort


    def default_sort(self):
        return self.sort(self.ENTERED_DATE, -1)


    def skip_to_page(self, page=1, size=200):
        return { '$skip': (page - 1) * size }


    def random_decay(self):
        x = random()
        n = 1
        while x > 0.5:
            x = random()
            n += 1
        return n


    def skip_to_random_page(self):
        return self.skip_to_page(self.random_decay())


    def limit_page(self, size=200):
        return { '$limit': size }


    account_nos_60k = [
        "110443519261",
        "397830443549",
        "799517638469",
        "438668948350",
        "754155843325",
        "167312313354",
        "545063539177",
        "526603697837",
        "033546218271",
        "142159486319",
        "547266229604",
        "579678098809",
        "377322831568",
        "081857526056",
        "172786528755",
        "211378559137",
        "155266997934",
        "976722699493",
        "687666868530",
        "623024311992"
    ]


    def search_60000_transactions(self):
        account_nos = choices(self.account_nos_60k, k=9)

        return { '$search': {
            'index': 'default',
            'compound': {
                'filter': [
                    self.account_no_in(account_nos),
                    self.date_range(datetime.now() - timedelta(days=30), datetime.now())
                ]
            },
            'sort': self.default_sort(),
            # 'sort': self.default_sort() if random() < 0.5 else self.random_sort(),
            'concurrent': True
        } }


    def search_100000_transactions(self):
        account_nos = choices(self.account_nos_60k, k=9)

        return { '$search': {
            'index': 'default',
            'compound': {
                'filter': [
                    self.account_no_in(account_nos),
                    self.date_range(datetime.now() - timedelta(days=50), datetime.now())
                ]
            },
            'sort': self.default_sort(),
            # 'sort': self.default_sort() if random() < 0.5 else self.random_sort(),
            'concurrent': True
        } }


    def quick_search_60000_transactions(self):
        account_nos = choices(self.account_nos_60k, k=9)

        return { '$search': {
            'index': 'default',
            'compound': {
                'filter': [
                    self.account_no_in(account_nos),
                    self.date_range(datetime.now() - timedelta(days=30), datetime.now())
                ],
                'must': [
                    {
                        'regex': {
                            'query': self.random_regex(),
                            'path': { 'wildcard': '*' },
                            'allowAnalyzedField': True
                        }
                    }
                ]
            },
            'sort': self.default_sort(),
            # 'sort': self.default_sort() if random() < 0.5 else self.random_sort(),
            'concurrent': True
        } }


    def quick_search_100000_transactions(self):
        account_nos = choices(self.account_nos_60k, k=9)

        return { '$search': {
            'index': 'default',
            'compound': {
                'filter': [
                    self.account_no_in(account_nos),
                    self.date_range(datetime.now() - timedelta(days=50), datetime.now())
                ],
                'must': [
                    {
                        'regex': {
                            'query': self.random_regex(),
                            'path': { 'wildcard': '*' },
                            'allowAnalyzedField': True
                        }
                    }
                ]
            },
            'sort': self.default_sort(),
            # 'sort': self.default_sort() if random() < 0.5 else self.random_sort(),
            'concurrent': True
        } }


    def random_character(self):
        chars = ascii_lowercase + ascii_uppercase + digits
        return choice(chars)

    # 50% chance of using a random 3 character string
    # 50% chance of using a random character followed by a wildcard followed by a random character
    def random_regex(self):
        if random() < 0.5:
            return ''.join([self.random_character() for _ in range(1, 3)])
        else:
            return self.random_character() + '.*' + self.random_character()


    sort_fields = [
        ACCOUNT_NUMBER,
        'CstRf',
        'TxTp',
        ENTERED_DATE,
        'TxEntDt',
        'Amt',
        'Ccy',
        'Ibn'
    ]


    # 50% chance of using a single sort field
    # 50% chance of using two sort fields
    # random direction for each field
    def random_sort(self):
        if random() < 0.5:
            return self.sort(choice(self.sort_fields), choice([1, -1]))
        else:
            sorts = choices(self.sort_fields, k=2)
            return self.sort(
                sorts[0], choice([1, -1]),
                sorts[1], choice([1, -1])
            )


    ################################################################
    # Start defining tasks and assign a weight to it.
    # All tasks need the @task() notation.
    # Weights indicate the chance to execute, e.g. 1=1x, 5=5x, etc.
    # In locustfile-mimesis.py, the task weights
    # have been parameterized too and dynamically passed in via Host
    # TODO Create any additional task functions here
    ################################################################


    @task(0)
    def _async_100000_filter_search(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "100000TransactionsFilter";

        try:
            # Get the record from the target collection now
            self.coll.aggregate([
                self.search_100000_transactions(),
                # self.skip_to_page(10),
                self.limit_page()
            ])
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0, exception=e)
            # Add a sleep so we don't overload the system with exceptions
            time.sleep(5)

    @task(5)
    def _async_100000_filter_database(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "100000TransactionsDatabase";

        try:
            # Get the record from the target collection now
            self.coll.find({
                self.ACCOUNT_NUMBER: { '$in': choices(self.account_nos_60k, k=9) },
                self.ENTERED_DATE: { '$gt': datetime.now() - timedelta(days=50) }
            }).sort({self.ENTERED_DATE: -1}).limit(200)
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0, exception=e)
            # Add a sleep so we don't overload the system with exceptions
            time.sleep(5)

    @task(5)
    def _async_100000_quick_search(self):
        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "100000TransactionsQuick";

        try:
            # Get the record from the target collection now
            self.coll.aggregate([
                self.quick_search_100000_transactions(),
                # self.skip_to_random_page(),
                self.limit_page()
            ])
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0, exception=e)
            # Add a sleep so we don't overload the system with exceptions
            time.sleep(5)
