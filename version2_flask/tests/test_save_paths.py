import os
os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/version2_flask')
import pytest
import helper_functions as hf
import pandas as pd
from markupsafe import Markup

in_prog, done = hf.get_save_paths('/this/is/fake/path',['this','is','fake','path'])
done

user='odairh'
def get_save_test1():
		in_prog, done = hf.get_save_paths('/this/is/fake/path',['this','is','fake','path'])
		assert in_prog == f'this/is/fake/path_{user}_inprogress'
		assert done == f'this/is/fake/path_odairh_done'
		print('new file test complete')
		
def get_save_test2():
		in_prog, done = hf.get_save_paths('/this/is/fake/path_odairh_inprogress',['this','is','fake','path','odairh','inprogress'])
		assert in_prog == f'this/is/fake/path_{user}_inprogress'
		assert done == f'this/is/fake/path_odairh_done'
		print('in prog test complete')
		
def get_save_test3():
		in_prog, done = hf.get_save_paths('/this/is/fake/path_odairh_done',['this','is','fake','path','odairh','done'])
		assert in_prog == f'this/is/fake/path_{user}_inprogress'
		assert done == f'this/is/fake/path_odairh_done'
		print('done test complete')
		
def get_save_test3():
		in_prog, done = hf.get_save_paths('/this/is/fake/path_jonesr_done',['this','is','fake','path','jonesr','done'])
		assert in_prog == f'this/is/fake/path_jonesr_{user}_inprogress'
		assert done == f'this/is/fake/path_jonesr_{user}_done'
		print('done test complete')
		