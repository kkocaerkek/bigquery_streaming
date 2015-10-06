#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import webapp2
import models
import datetime
import random


def create_sample_call_record():
    call = models.SimpleCallHistory()
    call.calldate = datetime.datetime.now()
    call.answered = random.choice([False, True])
    call.duration = random.randint(0, 3600) if call.answered else 0
    call.callerid = random.choice(["222", "333", "444"])
    call.called_number = random.choice(["555", "666", "777"])
    call.put()
    return call

class MainHandler(webapp2.RequestHandler):
    def get(self):
        call_record = create_sample_call_record()

        self.response.write('Call record created with key: %s' % unicode(call_record.key()))


app = webapp2.WSGIApplication([
    ('/', MainHandler)
], debug=True)
