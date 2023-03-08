import subprocess
import pandas as pd
import logging
from flask import Flask, render_template, request, redirect, \
url_for, flash, make_response, session
import os
os.chdir('/home/cdsw/Clerical_Resolution_Online_Widget/flask_poc')

app=Flask(__name__)

# disable logger to stop server logging
logging.getLogger('werkzeug').disabled=True

# read in clerical matching data. To be replaced by user input
working_file = pd.read_csv('/home/cdsw/Clerical_Resolution_Online_Widget/Data/clusters_example_data.csv')

# if match column exists in clerical file
if {'Match'}.issubset(working_file.columns):

    # variable indicates whether user has returned to this file (1) or not (0)
    matching_previously_began = 1

else:

    # create a match column and fill with blanks
    working_file['Match'] = ''

    matching_previously_began = 0
    
def update_index(self, event):
    '''
    This function updates the overall index variable which cycles through
    the Clerical Matching (CM) file. Additional functonality is directing
    to other functions to update the CM file and finally updating the GUI
    the next record to be clerically matched.
    Parameters
    ----------
    event : int - boolean
        This determines where to add a 1 or a 0 to the df
    Returns
    -------
    None.
     '''

    # Update the list of matching record IDs
    self.get_matches()

    # update the underlying dataframe with matching record IDs
    self.update_df(event)

    # Update the GUI labels
    self.update_gui(config, working_file)

    # reset match string so different pairings can be made in that cluster
    self.match_string = ''

    # clear the list of records in cluster remaining unmatched
    self.not_matched_yet.clear()

    # if match button has been clicked and there are still unmatched records in cluster
    if len(self.display_indexes) > self.current_num_cluster_decisions() and event == 1:
        pass

    # if no more matches can be made in cluster OR no more matches button is clicked
    else:

        # update the cluster_index and display indexes to referece the new cluster
        self.cluster_index += 1
        self.display_indexes = working_file.index[working_file['cluster_sequential_number'] == self.cluster_index].to_list(
        )
        self.len_current_cluster = len(self.display_indexes)

        stp_gui = self.check_matching_done()
        self.tags_container = {}

        # Check if reached the end of the script
        if stp_gui:
            pass
            # could add in additional functionality here to do with saving the working_file file
        else:
            # Update the GUI
            self.update_gui(config, working_file)

def check_matching_done(self):
    '''
    This function checks if the number of iterations is greater than the number of
    rows; and breaks the loop if so.
    Returns
    -------
    Boolean value, this dictates whether to stop displaying any more records
    and close the app or continue updating the app
    1 = Stop The GUI
    0 = Continue updating the GUI
    '''
    # Query whether the current record matches the total number of records
    if self.cluster_index > (self.num_clusters-1):
        # disable the match and Non-match buttons
        self.match_button.configure(state=tkinter.DISABLED)
        self.non_match_button.configure(state=tkinter.DISABLED)
        # inform the user that matching is finished
        self.matchdone = ttk.Label(
            root, text='Matching Finished. Press save and close.', foreground='red')
        self.matchdone.grid(row=1, column=0, columnspan=1)

        return 1
    else:

        return 0

def go_back(self):
    '''
    A function that goes back to the previous record.
    Returns
    -------
    None.
    '''
    # get the number of decisions made in current cluster
    num_decisions = self.current_num_cluster_decisions()

    # update cluster_index IF there are no descisions in current cluster
    if num_decisions == 0:
        self.cluster_index -= 1
        self.display_indexes = working_file.index[working_file['cluster_sequential_number'] == self.cluster_index].to_list(
        )

    # reset new (previous record) to empty strings
    for i in self.display_indexes:
        working_file.loc[i, 'Match'] = ""
        working_file.loc[i, 'Comments'] = ""

    # clean the match string
    self.match_string = ''

    # clear list of not matched yet
    self.not_matched_yet.clear()

    # spdate the gui
    self.update_gui(config, working_file)

    # configure match_buttons to normal
    self.match_button.config(state='normal')
    self.non_match_button.config(state='normal')

    # handling when user presses back button on the first cluster in the data
    try:
        self.matchdone.destroy()
    except AttributeError:
        pass

def update_df(self, event):
    '''
    Updates the dataframe with the matching outcome when the match button is selected
    Parameters
    ----------
    event : integer
        1 if match button pressed by user, 0 if no more matches button pressed
    Return
    -------
    None.
    '''
    # create a list of checkboxes ticked by the user
    checkboxes_selected = self.match_string.split(",")

    # if match button pressed
    if event == 1:

        # for each record in the current cluster
        for i in self.display_indexes:

            # if 1 or 0 records are selected, present user with warning
            if len(checkboxes_selected) <= 2:

                tkinter.messagebox.showwarning(
                    message="Two or more records must be selected to make a match")

                break

            # otherwise if the match column is currently empty
            if working_file.loc[i, 'Match'] == "":

                # if a record is selected by checkbutton
                if working_file[config['record_id_col']['record_id']][i] in self.match_string:

                    # append currently selected records' record ids to the match column
                    working_file.loc[i, 'Match'] = self.match_string

                    # remove matched records in cluster from list of those not yet matched
                    try:
                        self.not_matched_yet.remove(i)

                    # for cases where matched records have already been removed (big clusters)
                    except ValueError:

                        pass

                    # if commentbox specified in config
                    if int(config['custom_settings']['commentbox']):

                        # for each row where checkbox selected, append the commentbox contents
                        working_file.loc[i,
                                         'Comments'] = self.comment_entry.get()

                # append those not selected by checkbutton to list of those not yet matched
                else:

                    self.not_matched_yet.append(i)
            else:
                pass

        # if there are 1 or 0 records remaining without matching decisions
        if (len(self.display_indexes) - self.current_num_cluster_decisions()) <= 1:

            # for this remaining record, mark as a non-match
            for i in self.not_matched_yet:

                working_file.loc[i, 'Match'] = "No match in cluster"

    # if non-match button clicked
    else:
        # mark each record in cluster that has a null match decision as a non-match
        for i in self.display_indexes:

            if working_file.loc[i, 'Match'] == "":

                working_file.loc[i, 'Match'] = "No match in cluster"

                # if commentbox specified in config
                if int(config['custom_settings']['commentbox']):

                    working_file.loc[i,
                                     'Comments'] = self.comment_entry.get()

def current_num_cluster_decisions(self):
    '''
    Returns the number of records in the currently selected cluster that have been marked
    as a match.
    Parameters
    ----------
    None
    Return
    -------
    current_num_cluster_decisions: integer
    '''
    # list containing all record IDs for records marked as matches in current cluster
    current_cluster_decisions = [
        working_file.loc[i, 'Match'] for i in self.display_indexes]

    # counting records as a match if there's something in their corresponding match column
    current_num_cluster_decisions = len(
        [record for record in current_cluster_decisions if record != ""])

    return current_num_cluster_decisions
  
def get_matches(self):
    """
    A function that generates a string based on the matches identified in a cluster.
    Returns
    -------
    None.
    """
    # create, as a local variable, the list of matches within the current cluster
    list_of_matches = []

    # add to the list of matches; the index of any that are selected by the checkbox.
    for v, display_i in enumerate(self.display_indexes):

        status = eval(f'self.check_{v}.get()')
        if status:
            list_of_matches.append(display_i)

    # creates a string that is the record id's that match; separated by a comma
    # assign string to global variable self.match_string
    for i in list_of_matches:
        temp_string = str(
            working_file[config['record_id_col']['record_id']][i]) + ','
        self.match_string = self.match_string + temp_string
      
#extract list of cluster ids and separate out into dataframes

#####working poc#######
#######################
#working proof of concept
app= Flask(__name__)

app=Flask(__name__)

# disable logger to stop server logging
logging.getLogger('werkzeug').disabled=True

# read in clerical matching data. To be replaced by user input
working_file = pd.read_csv('/home/cdsw/Clerical_Resolution_Online_Widget/Data/clusters_example_data.csv')

@app.route('/', methods=['GET','POST'])
def intro():
#  session['input_df']=data_pd
  button = request.form.get("hdfs")
  return render_template("intro_page.html", button = button)

@app.route('/cluster_version', methods=["GET","POST"])
def index():
#    if button == 'hdfs':
#       process = ['hadoop', 'fs', '-get' , f'{hdfs_path}']
    
    
    
    
    if 'index' not in session:
        session['index'] = 201 # setting session data


    data_pd=session['input_df']
    cluster=working_file.loc[data_pd['Cluster_Number']==session['index']]
    
    if request.form.get('NEXT')=="no more matches":
        df['match_status'] = 'no match'

        session['index'] = session['index']+ 1
        
    df.to_csv("data.csv", index=False)
        
    return render_template("cluster_version.html",tables=[df.to_html(classes='data')], titles=df.columns.values)

if __name__ == "__main__":
    
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.config['SECRET_KEY']='abcd'
    app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))


##########################
##########################
## class based view test
#from flask.views import View
#
##working proof of concept
#app= Flask(__name__)
#app.config['SECRET_KEY']='abcd'
#
#@app.route('/', methods=['GET','POST'])
#def intro():
#   # session['input_df']=data_pd
#    return render_template("intro_page.html")
#
#
#class clerical_app(View):
#  
#  methods=["GET","POST"]
#  
#  def index():
#      if 'index' not in session:
#          session['index'] = 201 # setting session data
#
#
#      #data_pd=session['input_df']
#      cluster=working_file.loc[data_pd['Cluster_Number']==session['index']]
#
#      if request.form.get('NEXT')=="no more matches":
#          working_file['match_status'] = 'no match'
#
#          session['index'] = session['index']+ 1
#
#      cluster.to_csv("data.csv", index=False)
#
#      return render_template("cluster_version.html",tables=[cluster.to_html(classes='data')], titles=cluster.columns.values)
#
#app.add_url_rule('/cluster_version', view_func=clerical_app.as_view('cluster_version'))
#
#if __name__ == "__main__":
#    
#    app.config["TEMPLATES_AUTO_RELOAD"] = True
#    app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))

  
"""  
# Create DataFrame
app= Flask(__name__)
app.config['SECRET_KEY']='abcd'
@app.route('/', methods=['GET','POST'])
def intro():
    session['input_df']=request.files['file']
    return render_template("intro_page.html")

@app.route('/cluster_version', methods=["GET","POST"])
def index():
    if 'index' not in session:
        session['index'] = 201 # setting session data
    else:
        if request.form.get('NEXT')=="add one":
            session['index'] = session.get('index') + 1
    data_pd=session['input_df']
    df=data_pd.loc[data_pd['Cluster_Number']==session['index']]
    return render_template("cluster_version.html",tables=[df.to_html(classes='data')], titles=df.columns.values)
  
app.config["TEMPLATES_AUTO_RELOAD"] = True
app.run(host=os.getenv('CDSW_IP_ADDRESS'),port= int(os.getenv('CDSW_PUBLIC_PORT')))
"""
