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
$ python -c 'import polars as pl; pl.scan_csv("current.melt.tsv", separator="\t",
             dtypes={
                "SubjectID": pl.Int64,
                "FieldID": pl.Int64,
                "InstanceID": pl.Int64,
                "ArrayID": pl.Int64,
                "FieldValue": pl.Utf8,
            },  encoding="utf8-lossy").sink_ipc("current.melt.arrow", compression="zstd")'
```

## Requirements

The python script requires the [polars package](https://github.com/pola-rs/polars)

## Extracting data

Now that you have a `arrow` or `tsv` file ready, you can write a configuration
file to define the data you would like to extract and run the script.

To choose variables, use the [UKBB Showcase](https://biobank.ndph.ox.ac.uk/showcase/) to collect
`FieldIDs` and `Categories`

Make a copy of [config.template.yaml](config.template.yaml) and set your settings
as appropriate, see embedded documentation of the file for details.

You will need the `Codings.tsv` and `Data_Dictionary_Showcase.tsv` from
[UKBB Showcase Accessing Data](https://biobank.ndph.ox.ac.uk/showcase/exinfo.cgi?src=AccessingData)

UKBB Category tree file (Schema 13), tab-separated from https://biobank.ndph.ox.ac.uk/showcase/schema.cgi?id=13
and
UKBB Data field properties file (Schema 1), tab-separated from https://biobank.ndph.ox.ac.uk/showcase/schema.cgi?id=1

### Command-line use

And finally run the script, assuming you have the support files in the local directory:

```sh
$ python melted_UKBB_extract.py --config-file myconfig.yaml --data-file current.melt.arrow --output-prefix mysubset_
```

### Use inside python

The function `extract_UKBB_tabular_data` has the following signature:

```python
def extract_UKBB_tabular_data(
    config: Config,
    data_file: str | None = None,
    dictionary_file: str | None = None,
    coding_file: str | None = None,
    verbose: str | None = False,
) -> tuple[pl.DataFrame, pl.DataFrame | None, pl.DataFrame, pl.DataFrame]:
```

The return signature depends on the `config['wide']` setting.

```python
return data, data_wide, dictionary, codings
# Or when wide=False
return data, None, dictionary, codings
```

The `Config` class is a `TypedDict`, which is just a regular dictionary with defined types. This allows us to better document the properties of the dictionary. The properties of this dictionary are provided in `config.py`. If you want to have autocompletion in your IDE, you can create a config dict as follows:

```python
config: Config = {...}
```

If you want to load your config from a `yaml` file, you can do so as follows:

```python
config = load_config('config.template.yaml')
```

### Outputs

The script will use your config file and your `--output-prefix` to produce subsets of the full tabular data
in narrow (and if configured, wide) formats, as well as a filtered version of `Coding.tsv`
and `Data_Dictionary_Showcase.tsv` describing the data.

## Full Script Options

```sh
usage: UKBB Data Extractor [-h] --config-file CONFIG_FILE --data-file DATA_FILE [--dictionary-file DICTIONARY_FILE] [--coding-file CODING_FILE] [--category-tree-file CATEGORY_TREE_FILE] [--data-field-prop-file DATA_FIELD_PROP_FILE] --output-prefix OUTPUT_PREFIX [--output-formats [OUTPUT_FORMATS ...]] [-v]

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
                        UKBB coding file (default: Codings.tsv)
  --category-tree-file CATEGORY_TREE_FILE
                        UKBB Category tree file (Schema 13), tab-separated from https://biobank.ndph.ox.ac.uk/showcase/schema.cgi?id=13 (default: 13.txt)
  --data-field-prop-file DATA_FIELD_PROP_FILE
                        UKBB Data field properties file (Schema 1), tab-separated from https://biobank.ndph.ox.ac.uk/showcase/schema.cgi?id=1 (default: 1.txt)
  --output-prefix OUTPUT_PREFIX
                        Prefix for output files (default: None)
  --output-formats [OUTPUT_FORMATS ...]
                        Specify list of output file formats from tsv, arrow/feather, parquet, csv (default: ['tsv', 'arrow'])
  -v, --verbose         increase output verbosity (default: False)
```

## TODO

Regarding the return type of `extract_UKBB_tabular_data`, the function should ideally take a generic parameter for config that will determine the return type based on `config['wide']`. However, this is not possible in Python 3.10 ad below, as `TypedDict` cannot inherit from generic types ([see this for more info](https://github.com/python/cpython/issues/89026#issuecomment-1116093221)). Once Once Python 3.10 is no longer supported, this function could be updated with a generic type, so that the signature is inferred based on the value passed in for config.
