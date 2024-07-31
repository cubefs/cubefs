// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Request & Response in one Frame
//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +---------------------------------------------------------------+
// |                        Header Length                          |
// +---------------------------------------------------------------+
// :          Header (has Body Length and Trailer size)            :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
// :                         Body Bytes                            :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
// :                      Fixed Trailer Bytes                      :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +

// Request & Response in multi Frame
//  0                   1                   2                   3
//  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// +---------------------------------------------------------------+
// |                        Header Length                          |
// +---------------------------------------------------------------+
// :          Header (has Body Length and Trailer size)            :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
// :                         Body Bytes                            :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
//                              |
// :               Payload of Body in Frames                       :
//                              |
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
// :                         Body Continued                        :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
// :                      Fixed Trailer Bytes                      :
// + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +

package rpc2
