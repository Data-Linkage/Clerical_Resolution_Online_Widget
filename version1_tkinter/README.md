# CROW1

Here you will find everything you need to get started with the tkinter version
of CROW.

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Utils](#utils)
  - [Cluster-to-pairwise](#cluster-to-pairwise)
- [Help](#help)

## Requirements

- Python 3.9 or higher
- `tkinter` (included with standard Python installations)
- Additional dependencies listed in `requirements.txt`
- **Optional**: An [IDE][ide] (e.g. Spyder, Visual Studio (VS) Code, PyCharm
  etc.)

> [!NOTE]
>
> The following instructions include the command `python`. This command can only
> be used when the Python interpreter (`python.exe`) has been added to the
> [PATH][wikipedia-path] environment variable, or if you are working in a
> [virtual environment][python-docs-venv]. If neither of these are the case, you
> can add Python to your user environment variables, otherwise you will need to
> specify the full path to the Python interpreter instead of typing `python`.
> That is, rather than running the command:
>
> ```sh
> python your_script.py
> ```
>
> You would instead run:
>
> ```sh
> c:\path\to\your\python\interpreter your_script.py
> ```

## Installation

The commands listed in the following steps should be run in a terminal, for
example, [Command Prompt][wikipedia-cmd].

1. Clone the repository:

   ```sh
   git clone https://github.com/Data-Linkage/Clerical_Resolution_Online_Widget.git
   ```

2. Navigate to the location of the CROW1 scripts and configuration files:

   ```sh
   cd Clerical_Resolution_Online_Widget/version1_tkinter
   ```

3. Create and activate a virtual environment:

   ```sh
   python -m venv .venv
   ```

   and then

   ```sh
   .venv\Scripts\activate
   ```

4. Install dependencies:

   ```sh
   pip install -r requirements.txt
   ```

## Usage

More instructions can be found in the `docs` folder but the following can serve
as a "quickstart" guide.

Before you begin, you will need to choose between the clusters and pairwise
versions of CROW1. Your choice will depend on the format of the data that you
will be clerically reviewing.

> [!WARNING]
>
> Make sure your chosen script and its corresponding configuration file are in
> the same directory as one another. Otherwise the script will not be able to
> read the information in the configuration file.

1. Edit the configuration file (`Config_clusters.ini` or `Config_pairwise.ini`)
   to suit your data.
   A guide is embedded within each configuration file.
2. Start the application by running one of `CROW_clusters.py` or
   `CROW_pairwise.py` in an IDE or by running the command:

   ```sh
   python CROW_clusters.py
   ```

   or

   ```sh
   python CROW_pairwise.py
   ```

3. Load your data by choosing a CSV file when prompted.
4. Review your data and click `Save and Close` when done.

## Utils

The `utils` folder contains some scripts that may be useful when working with
CROW1. Each script will contain a module-level docstring at the top of the file
that will contain usage instructions.

### Cluster-to-pairwise

The `cluster_to_pairwise.py` script will convert data that works with the
cluster version of CROW1 into a format that works with the pairwise version. The
script will group the data by cluster and data source, and keep only the
unique information from each data source per cluster.

## Help

If you have additional questions or problems, please [check the
issues][crow-issues] or [open a new issue][crow-new-issue] according to the
[contributing guidelines][crow-contributing], or [contact us directly][email].

[crow-contributing]: ../CONTRIBUTING.md
[crow-issues]: https://github.com/Data-Linkage/Clerical_Resolution_Online_Widget/issues
[crow-new-issue]: https://github.com/Data-Linkage/Clerical_Resolution_Online_Widget/issues/new
[email]: mailto:linkage.hub@ons.gov.uk
[ide]: https://en.wikipedia.org/wiki/Integrated_development_environment
[python-docs-venv]: https://docs.python.org/3/library/venv.html
[wikipedia-cmd]: https://en.wikipedia.org/wiki/Cmd.exe
[wikipedia-path]: https://en.wikipedia.org/wiki/PATH_(variable)
