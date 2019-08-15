# CSV Parser

The csv parser can be used in common scenarios, and has easy-to-use APIs.

* It supports `\r` or `\n` or `\r\n` as line separator. So it can handle files with *nix/Windows line breaks.
* It supports quoting and escaping. Special characters like `\r`, `\n`, `,` and `"` can apprear in quoted fields.
* It support manually specified character as field separator(like `\t`) other than `,`.

See `csv.h` for more detailed documents. And see examples for how to use it.

TODO:
* automated tests.
