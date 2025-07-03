# CROW2

Here you will find everything you need to get started with the Flask version of
CROW.

> [!IMPORTANT]
>
> CROW2 was built for internal Office for National Statistics (ONS) access.
> While it may still function if you have access to the required software, it is
> not currently designed to be used outside of the ONS and, as such, we cannot
> provide technical guidance if you are trying to use CROW2 without being a
> member of the ONS. Please consider using CROW1 instead.

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

Before you begin, you will need to make sure your data is in the correct format
for CROW2.

## Help

If you have additional questions, or issues, please contact:
<linkage.hub@ons.gov.uk>

[dapcats-cdp]: https://gitlab-app-l-01/DAP_CATS/cdp-guidance-wiki/-/wikis/home
