// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Apache.Iggy.Kinds;

namespace Apache.Iggy.Tests.UtilityTests;

public sealed class IdentifiersByteSerializationTests
{

    [Fact]
    public void StringIdentifer_WithInvalidLength_ShouldThrowArgumentException()
    {
        const char character = 'a';
        string val = string.Concat(Enumerable.Range(0, 500).Select(_ => character));

        Assert.Throws<ArgumentException>(() => Identifier.String(val));
    }

    [Fact]
    public void KeyEntityId_WithInvalidLength_ShouldThrowArgumentException()
    {
        const char character = 'a';
        string val = string.Concat(Enumerable.Range(0, 500).Select(_ => character));

        Assert.Throws<ArgumentException>(() => Partitioning.EntityIdString(val));
    }

    [Fact]
    public void KeyBytes_WithInvalidLength_ShouldThrowArgumentException()
    {
        byte[] val = Enumerable.Range(0, 500).Select(x => (byte)x).ToArray();
        Assert.Throws<ArgumentException>(() => Partitioning.EntityIdBytes(val));
    }
}