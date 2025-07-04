# CROW2

> [!WARNING]
>
> CROW2 is currently encountering some issues and is in the process of being
> fixed. Please consider using [CROW1][crow1] until these issues are resolved.

Here you will find everything you need to get started with the Flask version of
CROW.

> [!IMPORTANT]
>
> CROW2 was built for internal Office for National Statistics (ONS) access.
> While it may still function if you have access to the required software, it is
> not currently designed to be used outside of the ONS and, as such, we cannot
> provide technical guidance if you are trying to use CROW2 without being a
> member of the ONS. [CROW1][crow1] is available as a substitute for external users.

- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Help](#help)

## Requirements

- Python 3.9 or higher
- [Cloudera Data Platform][dapcats-cdp] (CDP) access
- Dependencies listed in `requirements.txt`

## Installation

1. Start a Cloudera AI session in the project where CROW2 will be used.
2. Open a terminal by clicking `Terminal Access` in the top right-hand corner of
   the screen, above the session output pane.
3. Clone the repository:

   ```sh
   git clone https://github.com/Data-Linkage/Clerical_Resolution_Online_Widget.git
   ```

4. Navigate to the location of the CROW2 script and configuration file:

   ```sh
   cd Clerical_Resolution_Online_Widget/version2_flask
   ```

5. Install dependencies:

   ```sh
   pip install -r requirements.txt
   ```

## Usage

Before you begin:

- Make sure your data is in the correct format for CROW2.
- Follow the [installation](#installation) instructions.

> [!IMPORTANT]
>
> Make sure the CROW2 script and its corresponding configuration file are in the
> same directory as one another. Otherwise the script will not be able to read
> the information in the configuration file.

1. If you have not already done so, start a Cloudera AI session in the project
   in which you installed CROW2.
2. Edit the configuration file (`config_flow.ini`) to suit your data. A guide is
   embedded within the configuration file.
3. Start the application, either by opening the CROW2 script
   (`flask_new_flow.py`) and clicking the `Run` button above the editor pane, or
   from the terminal:

   1. Open a terminal by clicking `Terminal Access` above the session output
      pane.
   2. Navigate to the directory containing the CROW2 script and configuration file:

      ```sh
      cd Clerical_Resolution_Online_Widget/version2_flask
      ```

   3. Run the script:

      ```sh
      python flask_new_flow.py
      ```

4. Open the application by selecting it from the drop-down menu in the top
   right-hand corner above the session output pane.
5. Load your data by choosing a parquet file from the drop-down menu.
6. Review your data and click `Save` and close the application when done.

## Help

If you have additional questions or problems, please [check the
issues][crow-issues] or [open a new issue][crow-new-issue] according to the
[contributing guidelines][crow-contributing], or [contact us directly][email].

[crow1]: ../version1_tkinter/
[crow-contributing]: ../CONTRIBUTING.md
[crow-issues]: https://github.com/Data-Linkage/Clerical_Resolution_Online_Widget/issues
[crow-new-issue]: https://github.com/Data-Linkage/Clerical_Resolution_Online_Widget/issues/new
[dapcats-cdp]: https://gitlab-app-l-01/DAP_CATS/cdp-guidance-wiki/-/wikis/home
[email]: mailto:linkage.hub@ons.gov.uk
