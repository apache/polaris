#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

import re
import sys

ADMONITION_RE = re.compile(r'>\s*\[!(NOTE|WARNING|IMPORTANT|TIP|CAUTION|ALERT)\](.*)')

def main(input_file, output_file):
    with open(input_file, "r") as f:
        lines = f.read().splitlines()

    new_lines = []
    in_admonition = False
    for line in lines:
        m = ADMONITION_RE.match(line)
        # New admonition starts
        if m:
            if in_admonition:
                new_lines.append('{{< /alert >}}')
            admonition_type = m.group(1).lower()
            remaining = m.group(2).strip()
            new_lines.append(f'{{{{< alert {admonition_type} >}}}}')
            if remaining:
                new_lines.append(remaining)
            in_admonition = True
            continue
        # Inside admonition and line continues with ">"
        if in_admonition and line.startswith(">"):
            # strip one leading "> " or ">"
            new_lines.append(line[2:] if line.startswith("> ") else line[1:])
            continue
        # Inside admonition but line no longer part of it
        if in_admonition:
            new_lines.append('{{< /alert >}}')
            in_admonition = False
        # Normal line
        new_lines.append(line)
    # Close final admonition if open
    if in_admonition:
        new_lines.append('{{< /alert >}}')
    with open(output_file, "w") as f:
        f.write("\n".join(new_lines) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <input_file> <output_file>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
