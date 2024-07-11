# CROW: Clerical Resolution Online Widget

## About
This repository contains the CROW, the Clerical Resolution Online Widget, an open-source project designed to help data linkers with their clerical matching needs once they have linked data together!

The CROW reads record data from parquet files in HDFS/Hue, presents it in an easy to read and compare format and has tools that can help the clerical matcher with decisions.

##The aims of the CROW are to:

Meet the clerical matching needs of both large and small scale projects.
Get clerical matching results faster compared to other free to us software.
Minimise error during clerical review by presenting record pair data one at a time and in a easy to read and compare format.
Be easy to use for the clerical matcher when reviewing record pairs and the clerical coordinator setting up the project.
Be open and transparent, so the community can use the software without restrictions.
Installation and Use
The CROW can be from CDSW inside and outside of DAP. CROW exists in the Data Linkage Repository in Gitlab (DAP) and Github.

## What is new?
The new version of CROW has been developed in Flask. The ‘old’ version of CROW was initially written as a python script using the package Tkinter. For existing users the, previous version of CROW is still availiable; in the version1_tkinter folder. The new Flask version is availiable in teh version2_flask folder.

We have moved to Flask from Tkinter because of its design and functionality limitations - it only runs on Desktop using Anaconda or Spyder and only imports CSV files. Whereas Flask can use HTML functionality which has made it more accessible.

The tool now has a ‘select all’ feature, a scroll bar, it can run in CDSW and interfaces directly with hdfs/hue/s3 buckets and imports parquet files. This does however mean that unlike the old version of CROW, the tool works through cdsw/hive/hdfs only and cannot be run in any python environment. There is potential for this in future releases.

Previously in the old version of CROW, users were able to launch two different versions of the tool – Pairwise and cluster - depending on whether they were working with pairs of records or clusters. The new version of CROW has been consolidated into a single tool.

## Documentation
The most up to date documentation can be found in the instructions folders for each version. Here you will be able to find instructions for setting the CROW up for your project and instructions you can give to your clerical matchers on how to run the CROW once it is set up.

## Help and feedback
If you have any feedback, questions, require more information, need help or for anything else please contact the CROW team directly:

Linkage.Hub@ons.gov.uk

## Videos (demos)
TBC

## Accessibility
The CROW team have attempted to follow and implement accessibility requirements to the best of their ability, but due to time and resource constraints it is not likely all requirements will be met in the first release. However, the CROW team aims for continuous development of the tool to meet accessibility requirements with future releases.

The accessibility requirements are adapted from the ‘Web Content Accessibility Guidelines (WCAG) 2.0’, which covers a wide range of recommendations for making Web content more accessible. For more information on this, please visit: https://accessibility.18f.gov/checklist/

Following these guidelines aims to make content accessible to a wider range of people with disabilities, including blindness and low vision, deafness and hearing loss, learning disabilities, cognitive limitations, limited movement, speech disabilities, photosensitivity and combinations of these.

The following features have been implemented in the latest release of CROW:

Font formatting and style can be changed by the user depending on their preference
Text size can be changed by zooming in and out
Arial font is size 12 is the default font size – for accessibility reasons
Brightness of items hovered over and then highlighted are ‘muted’ for users who find bright colours uncomfortable
The user interface can be zoomed into with no issues, magnifying what is on screen without making its contents illegible
‘Read aloud’ works on CROW if opened in Microsoft Edge – but not in Google Chrome
The following features have not been implemented in the latest release of CROW, but will be considered for future releases:

Keyboard-tab accessibility
Background colours can be changed by user depending on their preference
Text colour and size
Records that are no longer available after matching are ‘greyed out’ so it is clear to users
If you have further accessibility requirements/needs please contact the team directly.

## Acknowledgments
We are grateful to colleagues within the Data Linkage Hub and wider Office for National Statistics for providing support for this work, expert advice and peer review of this work.



