# UK Biobank Tabular Preprocessing

This software is intended to assist to transforming the raw tabular data
(`*tab`) files supplied by the UKBB as raw data into a format more suitable
for analysis in python/R/MATLAB

## Preparation

First the UKBB supplied ultra-wide format must be coerced into a long format:

```sh
$ awk -f ukb_awk/melt_tab.awk current.tab > current.melt.tsv
```

This processing does not take much RAM, but is IO-bound. The file after this
will be significantly smaller, as NAs will be dropped.

This file is usable, but still very large, one may wish to convert to a
binary format which includes compression and significantly decreases size,
this performs a streaming conversion which also significantly reduces memory
requirements:

```sh
$ python -c 'import polars as pl; pl.scan_csv("current.melt.tsv", dtypes={
                "SubjectID": pl.Int64,
                "FieldID": pl.Int64,
                "InstanceID": pl.Int64,
                "ArrayID": pl.Int64,
                "FieldValue": pl.Utf8,
            },  encoding="utf8-lossy").sink_ipc("current.melt.arrow", compression='zstd')'
```

## Extracting data

Now that you have a `arrow` or `tsv` file ready, you can write a configuration
file to define the data you would like to extract and run the script.

To choose variables, use the [UKBB Showcase](https://biobank.ndph.ox.ac.uk/showcase/)

Make a copy of [config.template.yaml](config.template.yaml) and set your settings
as appropriate, see embedded documentation of the file for details.

You will need the `Codings.tsv` and `Data_Dictionary_Showcase.tsv` from
[UKBB Showcase Accessing Data](https://biobank.ndph.ox.ac.uk/showcase/exinfo.cgi?src=AccessingData)

And finally run the script:
```sh
$ python melted_UKBB_extract.py --config-file myconfig.yaml --output-prefix mysubset_
```

### Outputs

The script will use your config file and your `--output-prefix` to produce subsets of the full tabular data
in narrow (and if configured, wide) formats, as well as a filtered version of `Coding.tsv`
and `Data_Dictionary_Showcase.tsv` describing the data.

## Full Script Options
```sh
usage: UKBB Data Extractor [-h] --config-file CONFIG_FILE --data-file DATA_FILE [--dictionary-file DICTIONARY_FILE] [--coding-file CODING_FILE]
                           --output-prefix OUTPUT_PREFIX [--output-formats [OUTPUT_FORMATS ...]] [-v]

Transforms melted UKBB tabular data into a usable DataFrame for statistical analysis

optional arguments:
  -h, --help            show this help message and exit
  --config-file CONFIG_FILE
                        YAML config file describing how to process UKBB table (default: None)
  --data-file DATA_FILE
                        UKBB melted tabular data (default: None)
  --dictionary-file DICTIONARY_FILE
                        UKBB data dictionary showcase file (default: Data_Dictionary_Showcase.tsv)
  --coding-file CODING_FILE
                        UKBB conding file (default: Codings.tsv)
  --output-prefix OUTPUT_PREFIX
                        Prefix for output files (default: None)
  --output-formats [OUTPUT_FORMATS ...]
                        Specify list of output file formats from tsv, arrow/feather, parquet, csv (default: ['tsv', 'arrow'])
  -v, --verbose         increase output verbosity (default: False)
```