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

using System.Text.RegularExpressions;

namespace Apache.Iggy.Tools.ErrorCodeGenerator;

/// <summary>
///     Represents a parsed error code from Rust.
/// </summary>
public sealed record RustErrorCode(string Name, int Code, string Message);

/// <summary>
///     Parses Rust IggyError enum from iggy_error.rs file.
/// </summary>
public static partial class RustErrorParser
{
    // Matches: #[error("message")] or #[error("message {0} {1}")]
    [GeneratedRegex(@"#\[error\(""([^""]+)""\)\]")]
    private static partial Regex ErrorAttributeRegex();

    // Matches: VariantName = 123, or VariantName(Type) = 123, or VariantName { field: Type } = 123,
    [GeneratedRegex(@"^\s*([A-Z][a-zA-Z0-9_]*)(?:\s*\([^)]*\)|\s*\{[^}]*\})?\s*=\s*(\d+)\s*,?\s*$")]
    private static partial Regex VariantRegex();

    /// <summary>
    ///     Parses the Rust iggy_error.rs file and extracts error codes.
    /// </summary>
    public static List<RustErrorCode> Parse(string rustFileContent)
    {
        var errors = new List<RustErrorCode>();
        var lines = rustFileContent.Split('\n');

        string? currentErrorMessage = null;

        foreach (var line in lines)
        {
            // Check for #[error("...")] attribute
            var errorMatch = ErrorAttributeRegex().Match(line);
            if (errorMatch.Success)
            {
                currentErrorMessage = errorMatch.Groups[1].Value;
                continue;
            }

            // Check for variant definition with code
            var variantMatch = VariantRegex().Match(line);
            if (variantMatch.Success && currentErrorMessage != null)
            {
                var name = variantMatch.Groups[1].Value;
                var code = int.Parse(variantMatch.Groups[2].Value);

                errors.Add(new RustErrorCode(name, code, currentErrorMessage));
                currentErrorMessage = null;
            }

            // Reset if we hit a line that doesn't continue the pattern
            if (!string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith("//") &&
                !line.TrimStart().StartsWith("#[") && !variantMatch.Success)
            {
                currentErrorMessage = null;
            }
        }

        return errors;
    }

    /// <summary>
    ///     Parses the Rust file from the given path.
    /// </summary>
    public static async Task<List<RustErrorCode>> ParseFileAsync(string filePath)
    {
        var content = await File.ReadAllTextAsync(filePath);
        return Parse(content);
    }
}
