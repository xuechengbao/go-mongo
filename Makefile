# Copyright 2010 Gary Burd
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

include $(GOROOT)/src/Make.inc

TARG=github.com/garyburd/go-mongo
GOFILES=\
    buffer.go\
    bson.go\
    bson_decode.go\
    bson_encode.go\
    mongo.go\
    connection.go\
    pool.go\
    log.go\
    database.go\
    collection.go\
    query.go\
    deprecated.go\
 
include $(GOROOT)/src/Make.pkg
