# Copyright 2020 The ChubaoFS Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.

# -*- coding: utf-8 -*-
import sys


def SpiltFile():
    dicts = {}
    args = sys.argv
    originFile = args[-2]
    ouputFile = args[-1]

    with open(originFile) as covFileHandle:
        for line in covFileHandle:
            eachLine = line.strip("\n")

            if "atomic" in eachLine:
                continue

            eachKey = "_".join(eachLine.split(" ")[:-1])
            eachValue = int(eachLine.split(" ")[-1])

            if eachKey in dicts:
                dicts[eachKey] += eachValue
            else:
                dicts[eachKey] = eachValue

    sorted(dicts)
    with open(ouputFile, mode="w") as summaryfileHandle:
        summaryfileHandle.write("mode: atomic\n")
        for k, v in dicts.items():
            summaryfileHandle.write("%s %s\n" % (k.replace("_", " "), v))

    print("%s has done, result in %s" % (originFile, ouputFile))


if __name__ == '__main__':
    SpiltFile()
