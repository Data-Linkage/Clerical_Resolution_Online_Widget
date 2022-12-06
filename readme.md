# CROW: Clerical Resolution Online Widget
This repository contains the CROW, the Clerical Resolution Online Widget, an open-source project designed to help data linkers with their clerical matching needs once they have linked data together!

The CROW reads record pair data from a CSV file, presents it in an easy to read and compare format and has tools that can help the clerical matcher with decisions. 

The aims of the CROW are to:

* Meet the clerical matching needs of both large and small scale projects. 
* Get clerical matching results faster compared to other free to us software.
* Minimise error during clerical review by presenting record pair data one at a time and in a easy to read and compare format. 
* Be easy to use for the clerical matcher when reviewing record pairs and the clerical coordinator setting up the project. 
* Be open and transparent, so the community can use the software without restrictions. 

## Installation and Use
The CROW is a piece of software that runs in Python, it uses Pandas to read and update the record pair data and it uses tkinter to display and let users interact with the record pair data. 

There are two versions of CROW hosted in this repository - `CROW_clusters.py` and `CROW_pairwise.py`. These are versions suited to clustered and pairwise linkage outputs, please pick the version best suited to your linkage outputs.

### Common problems
Some users have experienced issues when using long numbers as record IDs, such as those created by the monotonically_increasing_id function in PySpark. To get around this, users could append a character to the start of their record IDs or use a function such as uuid1() from the `uuid` module in Python, or the uuid() function in PySpark.

### Pairwise 
To install and use the pairwise version simply pull this repository into your current working environment, adapt the sections in the [config pairwise](http://gitlab-01-01/Data_Linkage/Clerical_Resolution_Online_Widget/-/blob/master/Config_pairwise.ini), test that the [CROW_pairwise.py](http://gitlab-01-01/Data_Linkage/Clerical_Resolution_Online_Widget/-/blob/master/CROW_pairwise.py) works by running it in a Python IDE of your choice, then you are all set. 

### Clusters
To install and use the cluster version, simply pull this repository into your current working environment, adapt the sections in the [config clusters](http://gitlab-01-01/Data_Linkage/Clerical_Resolution_Online_Widget/-/blob/master/Config_clusters.ini), test that the [CROW_clusters.py](http://gitlab-01-01/Data_Linkage/Clerical_Resolution_Online_Widget/-/blob/master/CROW_clusters.py) works by running it in a Python IDE of your choice, then you are all set.

To get the outputs from cluster version of CROW into a pairwise linked format, please use the [CROW cluster output updater script provided](http://gitlab-01-01/Data_Linkage/Clerical_Resolution_Online_Widget/-/blob/master/Instructions/CROW_cluster_output_updater.py)

## Documentation
### Pairwise
The most up to date documentation can be found in the [instructions folder](http://gitlab-01-01/Data_Linkage/Clerical_Resolution_Online_Widget/-/tree/master/Instructions). Here you will be able to find instructions for setting the CROW up for your project and instructions you can give to your clerical matchers on how to run the CROW once it is set up. 

### Clusters
In addition to the documentation for setting up the pairwise version, see specific [documentation on use of the cluster version](https://github.com/Data-Linkage/Clerical_Resolution_Online_Widget/blob/main/Instructions/Cluster%20Version%20Instructions.pdf) of CROW.

## Videos 
TBC 

## Acknowledgments 
We are grateful to colleagues within the Data Linkage Hub and wider Office for National Statistics for providing support for this work, expert advice and peer review of this work. 
