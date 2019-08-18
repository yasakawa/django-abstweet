from django.db import models, connection
from django.conf import settings
from datetime import datetime, timedelta
from email.utils import parsedate
from django.utils import timezone
import os
import socket
from django.core.exceptions import ObjectDoesNotExist

DEBUG = getattr(settings, 'DEBUG', False)
USE_TZ = getattr(settings, 'USE_TZ', True)

current_timezone = timezone.get_current_timezone()

def parse_datetime(string):
    if settings.USE_TZ:
        return datetime(*(parsedate(string)[:6]), tzinfo=current_timezone)
    else:
        return datetime(*(parsedate(string)[:6]))

class AbstractTweet(models.Model):
    """
    Selected fields from a Twitter Status object.
    Incorporates several fields from the associated User object.
    For details see https://dev.twitter.com/docs/platform-objects/tweets

    It doesn't store Geo parameters because they will be deprecatednot store 
    them because they will be deprecated.
    """

    class Meta:
        abstract = True

    id = fields.BigAutoField(primary_key=True)

    # Basic tweet info
    tweet_id = models.BigIntegerField()
    text = models.CharField(max_length=250)
    truncated = models.BooleanField(default=False)
    lang = models.CharField(max_length=9, null=True, blank=True, default=None)

    # Basic user info
    user_id = models.BigIntegerField()
    user_screen_name = models.CharField(max_length=50)
    user_name = models.CharField(max_length=150)
    user_verified = models.BooleanField(default=False)

    # Timing parameters
    created_at = models.DateTimeField(db_index=True)  # should be UTC
    user_utc_offset = models.IntegerField(null=True, blank=True, default=None)
    user_time_zone = models.CharField(max_length=150, null=True, blank=True, default=None)

    # none, low, or medium
    filter_level = models.CharField(max_length=6, null=True, blank=True, default=None)

    # Engagement - not likely to be very useful for streamed tweets but whatever
    favorite_count = models.PositiveIntegerField(null=True, blank=True)
    retweet_count = models.PositiveIntegerField(null=True, blank=True)
    user_followers_count = models.PositiveIntegerField(null=True, blank=True)
    user_friends_count = models.PositiveIntegerField(null=True, blank=True)

    # Relation to other tweets
    in_reply_to_status_id = models.BigIntegerField(null=True, blank=True, default=None)
    retweeted_status_id = models.BigIntegerField(null=True, blank=True, default=None)

    @property
    def is_retweet(self):
        return self.retweeted_status_id is not None

    @classmethod
    def create_from_json(cls, raw):
        """
        Given a *parsed* json status object, construct a new Tweet model.
        """

        user = raw['user']
        retweeted_status = raw.get('retweeted_status')
        if retweeted_status is None:
            retweeted_status = {'id': None}

        # Replace negative counts with None to indicate missing data
        counts = {
            'favorite_count': raw.get('favorite_count'),
            'retweet_count': raw.get('retweet_count'),
            'user_followers_count': user.get('followers_count'),
            'user_friends_count': user.get('friends_count'),
            }
        for key in counts:
            if counts[key] is not None and counts[key] < 0:
                counts[key] = None

        return cls(
            # Basic tweet info
            tweet_id=raw['id'],
            text=raw['text'],
            truncated=raw['truncated'],
            lang=raw.get('lang'),

            # Basic user info
            user_id=user['id'],
            user_screen_name=user['screen_name'],
            user_name=user['name'],
            user_verified=user['verified'],

            # Timing parameters
            created_at=parse_datetime(raw['created_at']),
            user_utc_offset=user.get('utc_offset'),
            user_time_zone=user.get('time_zone'),

            # none, low, or medium
            filter_level=raw.get('filter_level'),

            # Engagement - not likely to be very useful for streamed tweets but whatever
            favorite_count=counts.get('favorite_count'),
            retweet_count=counts.get('retweet_count'),
            user_followers_count=counts.get('user_followers_count'),
            user_friends_count=counts.get('user_friends_count'),

            # Relation to other tweets
            in_reply_to_status_id=raw.get('in_reply_to_status_id'),
            retweeted_status_id=retweeted_status['id']
        )

    @classmethod
    def get_created_in_range(cls, start, end):
        """
        Returns all the tweets between start and end.
        """
        return cls.objects.filter(created_at__gte=start, created_at__lt=end)

    @classmethod
    def get_earliest_created_at(cls):
        """
        Returns the earliest created_at time, or None
        """
        result = cls.objects.aggregate(earliest_created_at=models.Min('created_at'))
        return result['earliest_created_at']

    @classmethod
    def get_latest_created_at(cls):
        """
        Returns the latest created_at time, or None
        """
        result = cls.objects.aggregate(latest_created_at=models.Max('created_at'))
        return result['latest_created_at']

    @classmethod
    def count_approx(cls):
        """
        Get the approximate number of tweets.
        Executes quickly, even on large InnoDB tables.
        """
        if settings.DATABASES['default']['ENGINE'].endswith('mysql'):
            query = "SHOW TABLE STATUS WHERE Name = %s"
            cursor = connection.cursor()
            cursor.execute(query, [cls._meta.db_table])

            desc = cursor.description
            row = cursor.fetchone()
            row = dict(zip([col[0].lower() for col in desc], row))

            return int(row['rows'])
        else:
            return cls.objects.count()
