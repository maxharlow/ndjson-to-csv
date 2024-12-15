NDJson-to-CSV
=============

**Superseded by [Entabulate](https://github.com/maxharlow/entabulate).**

Convert NDJson format data into CSV. Data is streamed, so files much bigger than the available memory can still be converted. Takes into account nested Json objects.


Installing
----------

    $ npm install -g ndjson-to-csv

Alternatively, don't install it and just prepend the command with `npx`.


Usage
-----

    $ ndjson-to-csv input.ndjson > output.csv

Where `input.ndjson` is your input data.
