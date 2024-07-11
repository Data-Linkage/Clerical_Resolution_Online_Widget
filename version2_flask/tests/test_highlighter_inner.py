import os
os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/version2_flask')
import pytest
import helper_functions as hf
import pandas as pd
from markupsafe import Markup



data1 = {'Name': ['John', 'Jon', ''],
       'Age': ['25', '30', '35'],
       'City': ['New York', 'London', 'Paris']}

data2 = {'Name': ['John', 'Jon',""],
       'Age': ['25', '30', '35'],
       'City': ['New York', 'London', 'Paris']}
data3 = {'Name': ['', 'Jon', ''],
       'Age': ['25', '35','l'],
       'City': ['New York', 'London', 'Paris']}

# create a dataframe from the dictionary
testdf1=pd.DataFrame(data1)
r1={'Name': {0: 'John', 1: Markup('Jo<mark>n</mark>'), 2: Markup('')},
	'Age': {0: '25',
  1: Markup('<mark>3</mark><mark>0</mark>'),
  2: Markup('<mark>3</mark>5')},
 'City': {0: 'New York',
  1: Markup('<mark>L</mark><mark>o</mark><mark>n</mark><mark>d</mark><mark>o</mark><mark>n</mark>'),
  2: Markup('<mark>P</mark><mark>a</mark><mark>r</mark><mark>i</mark><mark>s</mark>')},
 }
result1=pd.DataFrame(r1)

testdf2=pd.DataFrame(data2)
result2dict={
	'Name': {0: 'John', 1: Markup('Jo<mark>n</mark>'), 2: Markup('')},
	'Age': {0: '25',
  1: Markup('<mark>3</mark><mark>0</mark>'),
  2: Markup('<mark>3</mark>5')},
 'City': {0: 'New York',
  1: Markup('<mark>L</mark><mark>o</mark><mark>n</mark><mark>d</mark><mark>o</mark><mark>n</mark>'),
  2: Markup('<mark>P</mark><mark>a</mark><mark>r</mark><mark>i</mark><mark>s</mark>')}}
result2=pd.DataFrame(result2dict)


def test_highlighter_inner():
		display_test= hf.highlighter_inner(['Name','Age','City'],testdf1)
		assert display_test.equals(result1)
		display_test2= hf.highlighter_inner(['Name','Age','City'],testdf2)
		assert display_test2.equals(result2)
					

#things I have founf out= highlighter cannot handle nans
