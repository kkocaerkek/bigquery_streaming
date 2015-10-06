#!/usr/bin/env python
# -*- coding: utf-8 -*-

from google.appengine.ext import db, deferred
from streaming_insert import streaming_insert_call_history


class HookedModel(db.Model):
    def before_put(self):
        pass

    def after_put(self):
        pass

    def put(self, **kwargs):
        self.before_put()
        key = super(HookedModel, self).put(**kwargs)
        self.after_put()
        return key

    def before_delete(self):
        pass

    def after_delete(self):
        pass

    def delete(self, **kwargs):
        self.before_delete()
        super(HookedModel, self).delete(**kwargs)
        self.after_delete()


class SimpleCallHistory(HookedModel):
    calldate = db.DateTimeProperty()
    answered = db.BooleanProperty()
    duration = db.IntegerProperty()
    callerid = db.StringProperty()
    called_number = db.StringProperty()

    def before_put(self):
        self.__is_new_record = not self.is_saved()

    def after_put(self):
        if self.__is_new_record:
            """
             new record. big query streaming insert
            """
            deferred.defer(streaming_insert_call_history, self, _queue="BigQueryStreaming")