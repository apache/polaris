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
        content = f.read()
    # Extract frontmatter if present
    frontmatter = ""
    body = content
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) == 3:
            frontmatter = f"---{parts[1]}---"
            body = parts[2]
    # Keep only content starting from "Installation" section
    idx = body.find("## Installation")
    if idx != -1:
        body = body[idx:]
    # Convert Markdown admonitions to Hugo alerts
    lines = f"{frontmatter}\n{body}".splitlines()
    output_lines = []
    in_admonition = False
    for line in lines:
        match = ADMONITION_RE.match(line)
        # End admonition if we hit a non-admonition, non-quoted line
        if in_admonition and not (match or line.startswith(">")):
            output_lines.append("{{< /alert >}}")
            in_admonition = False
        if match:
            # Begin a new admonition
            admon_type = match.group(1).lower()
            remaining_text = match.group(2).strip()
            output_lines.append(f"{{{{< alert {admon_type} >}}}}")
            if remaining_text:
                output_lines.append(remaining_text)
            in_admonition = True
        elif in_admonition and line.startswith(">"):
            # Admonition body line
            output_lines.append(line[2:] if line.startswith("> ") else line[1:])
        else:
            # Regular line
            output_lines.append(line)
    # Close if file ends mid-admonition
    if in_admonition:
        output_lines.append("{{< /alert >}}")
    # Write
    with open(output_file, "w") as f:
        f.write("\n".join(output_lines) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <input_file> <output_file>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
