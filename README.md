# KIS Log Analysis - VBS 2022
This repo provides a good code base for extracting and analyzing the logs from the different teams that participated to VBS 2022.

Hopefully, this repo will help to analyze also the logs from the future versions of VBS.

## Data Preparation
First of all, run the following command. This will extract log data from 2022 teams and will prepare the environment.
```
./prepare.sh
source venv/bin/activate
```

## Generate a plot
A plot can be generated following the configuration written in a yaml configuration file.

This is an example call to construct the _time-recall_ table
```
python plot.py time_recall_table --config config2022.yaml
```

You can use the `--help` argument to see all the available options. Note that, in order to optimize the data processing, two levels of cache are used. You can disable caching using the `--no_result_cache` and `--no_log_cache` options.

## Add custom plots
Adding a custom plot is quite straightforward. These are the steps:

1. Inside the `generate` folder, create a .py file containing a class extending the `Result` class. In particular, you have to provide the methods:
    - `_generate()`: here you generate a nice view of the team log data, returning a Pandas dataframe. This dataframe is automatically cached and re-used (only if `--no_result_cache` is not set).
    - `_render(df)`: it renders the dataframe into a graph or a table.
3. Expose this class in the `generate/__init__.py` file.
2. Add a corresponding entry in the yaml configuration file.

Look at the `time_recall_table.py` file for an example.

## Contributors

Lucia Vadicamo - [lucia.vadicamo@isti.cnr.it](mailto:lucia.vadicamo@isti.cnr.it)

Nicola Messina - [nicola.messina@isti.cnr.it](mailto:nicola.messina@isti.cnr.it)
