"""
Welcome to CROW, the Clerical Resolution Online Widget, an open source project
designed to help researchers, analysts and statiticians with their clerical
matching needs once they have linked data together. 

This is the master CROW python script that can easily be adapted to your linkage 
project using the Config.ini file. To make the CROW work please edit the Config 
file, its easier to read and will save you time! 

Once you have adapted the Config file and have tested it. Put this master CROW
python script along with your adapted config file in a shared common area 
so the rest of your clerical matchers can access it. DONT forget to save this
file as read-only. And then you are done! 

More detail on these steps can be found on the Data Integration Sharepoint, 
including video documentation. 

Script was orginally created on Wed May 26 2021

Please get in contact using the below details if you have any questions:

Hannah O'Dair -- Hannah.O'Dair@ons.gov.uk -- Co-Lead Developer

Anthony G Edwards -- Anthony.G.Edwards@ons.gov.uk -- Co-Lead Developed

Craig Scott -- Craig.Scott@ons.gov.uk -- Creator 
You can also contact the linkage hub at: 
    
linkage.hub@ons.gov.uk
_________
We would like to acknowledge and thank David Cobbledick and Andrew Sutton for reviewing this code.
"""
from tkinter import *
from tkinter import messagebox
from tkinter import ttk
from tkinter import filedialog as fd
import os
import getpass
import pandas as pd
import configparser 

########## Intro GUI 

class IntroWindow:
    '''
    intro_window class function - opens a window that prompts the user to 
    choose their matching file.
    
    '''
    def  __init__(self, root, init_dir, files_info):
        
        # Initialise the gui paramaters 
        root.geometry('400x225')
        root.title('Clerical Matching')
        
        # initialise some variables
        self.init_dir = init_dir 
        self.files_info = files_info 
        
        # initialise the frame 
        self.content = ttk.Frame(root)
        self.frame = ttk.Frame(self.content, borderwidth=5, relief='ridge', width=500, height=300)
        self.content.grid(column=0, row=0)
        self.frame.grid(column=0, row=0, columnspan=5, rowspan=5)
        
        # create some widgets and place them on the gui
        self.intro_text = ttk.Label(self.content, text='Welcome to the Clerical Matching Application. \nPlease click "Choose File" to select your file \nand begin matching.',font='Helvetica 10')
        self.intro_text.grid(row=1, column=0, columnspan=4)
        
        # create the button
        self.choose_file_button = ttk.Button(self.content, text='Choose File', command=lambda: self.open_dirfinder() )
        self.choose_file_button.grid(row=2, column=1, columnspan=1, sticky='new')
    
    def open_dirfinder(self):
        '''
        Opens a file select window, allows user to choose a file. Ends the GUI. 

        Returns
        -------
        None.

        '''
        # Open up a window that allows the user to choose a matching file
        self.fileselect = fd.askopenfilename(initialdir=self.init_dir, title="Please select a file:", filetypes=self.files_info)
        
        # close down intro_window
        root.destroy()
        

class ClericalApp:
    '''
    ClericalApp class function - opens a window and allows users to clerically review records. 
    
    '''    

    def __init__(self, root, working_file, filename_done, filename_old, config): 

        
        # Create a title
        root.title('Clerical Matching')
        
        # Create the separate frames 
        # 1 - Tool Frame 
        toolFrame = ttk.LabelFrame(root, text='Tools:')
        toolFrame.grid(row=0, column=0, columnspan=1, sticky = 'ew', padx=10, pady=10)
        
        # 2 - Record Frame
        recordFrame = ttk.LabelFrame(root, text='Current Record')
        recordFrame.grid(row=2, column=0, columnspan=1, padx=10, pady=3)
        
        # initalise the file name variables
        self.filename_done = filename_done

        self.filename_old = filename_old
        
        self.not_matched_yet = []
        
        # create protocol for if user presses the 'X' (top right) 
        root.protocol("WM_DELETE_WINDOW", self.on_exit)
    
        # create a match column if one doesn't exist
        # replace any missing values (NA) with blank spaces
        if {'Match'}.issubset(working_file.columns):
            
            # convert all columns apart from Match and Comments (if specified) to string
            for col_header in working_file.columns:
                
                if (col_header == 'Match') or (col_header == 'Comments'):
                    pass
                else:
                    # convert to string
                    working_file[col_header] = working_file[col_header].astype(str)
                    # remove nan values 
                    for i in range(len(working_file)):
                        
                        if working_file[col_header][i] == 'nan':
                            
                            working_file.at[i,col_header] = ''
                        
                    
            working_file.fillna('', inplace=True)
            
            # variable indicates whether user has returned to this file or not
            self.matching_previously_began = 1
        else:
            working_file['Match'] = ''            
            # Create the comment box column if column header is specified
            if int(config['custom_settings']['commentbox']):
                working_file['Comments'] = ''

            # convert all columns apart from Match and Comments (if specified) to string
            for col_header in working_file.columns:
                
                if (col_header == 'Match') or (col_header == 'Comments'):
                    pass
                else:
                    # convert to string
                    working_file[col_header] = working_file[col_header].astype(str)
                    # remove nan values 
                    for i in range(len(working_file)):
                        
                        if working_file[col_header][i] == 'nan':
                            
                            working_file.at[i,col_header] = ''          
            
            working_file.fillna('', inplace=True)
            
            self.matching_previously_began = 0
        
        # count how many records are in the CM file
        self.num_records = len(working_file)
        
        #a counter of the number of checkpoint saves. 
        self.checkpointcounter = 0
        
        # create a sequential cluster id number from the cluster id variable
        cluster_var=config['cluster_id_number']['cluster_id']
        working_file['cluster_sequential_number'] = pd.factorize(working_file[cluster_var])[0]
        clusters_to_iterate=list(working_file['cluster_sequential_number'].unique())
        
        # counter variable for iterating through the CM file
        #For multiple records version use cluster ids

        #get the starting cluster id: 
        self.cluster_index= self.get_starting_cluster_id()
        #create a variable to indicate the lumber of cluster id's
        self.num_clusters=len(clusters_to_iterate)
        #get a list of the indices of the records contained within the current cluster.
        self.display_indexes= working_file.index[working_file['cluster_sequential_number']==self.cluster_index].to_list()
        
        #create a variable that indicates the length of the current cluster
        self.len_current_cluster = len(working_file['cluster_sequential_number']==self.cluster_index)
        
        # create an empty string to record results
        self.match_string = ''
        
        # create the text size component
        self.text_size = 10
        self.text_bold_boolean = 0
        self.text_bold = ''
        
        # show or hide differences boolean
        #self.show_hide_diff = 0
       # self.difference_col_label_names = {}

        # Create the record_index matches
        try:
            self.counter_matches = ttk.Label(recordFrame, text=f'{self.cluster_index+1} / {self.num_clusters} Clusters', font='Helvetica 9')
            self.counter_matches.grid(row=0, column=len(config.options('column_headers_and_order')), columnspan=1, padx=10, sticky="e") 

        except: 
            
            TypeError
            messagebox.showinfo(title = "Matching completed", message = "Please select a different file to clerically match")
            
            # close down the application
            root.destroy()
        
        # Create empty lists of labels 
        self.non_iterated_labels = []
        self.iterated_labels = []
               
        # ---------------------
        # Create dataset name widgets and separators between records
        row_adder = 0
        separator_adder = 2
    
        # ---------------------
  
        # Create column header labels and place all them on row 1, column n+2 (where n == the enumerate of the list)
        for n, column_title in enumerate(config.options('column_headers_and_order')):
            
            # Remove spaces from the user input and split them into different components

            col_header = config['column_headers_and_order'][column_title].replace(' ','').split(',')
            
            exec(f'self.{column_title} = ttk.Label(recordFrame,text="{col_header[0]}",font=f"Helvetica {self.text_size} bold")')

            exec(f'self.{column_title}.grid(row=1,column=n+1,columnspan=1, sticky = W, padx=10, pady=3)')
            
            # Add the executed self.labels for the column headers to the non_iterated_labels list
            self.non_iterated_labels.append(column_title)
            
        # ---------------------
        #create an empty list which will contain the name of all the checkbutton variables as a string
        self.check_to_append = []

        #iterate over column info and order. 
        for n, columnfile_title in enumerate(config.options('columnfile_info_and_order')):
            for v, display_i in enumerate(self.display_indexes):
            
                col_header = config['columnfile_info_and_order'][columnfile_title].replace(' ','').split(',')
            
                # create a text label
                exec(f'self.{col_header[0]}row{v} = Text(recordFrame,height=1,relief="flat",bg="gray93")')
                
                # Enter in the text from the df
                exec(f'self.{col_header[0]}row{v}.insert("1.0",working_file["{col_header[0]}"][{display_i}])')
                
                # configure Text so that it is a specified width, font and cant be interacted with
                exec(f'self.{col_header[0]}row{v}.config(width=len(working_file["{col_header[0]}"][{display_i}])+10,font=f"Helvetica {self.text_size}",state=DISABLED)')
                
                #grid the text label to the widget. 
                exec(f'self.{col_header[0]}row{v}.grid(row={v+2}, column={n+1},columnspan=1,padx=10, pady=3,sticky="w")')
                
                #create a checkbutton and append it to the list of checkbutton variables. 
                exec(f'self.check_{v}= IntVar()')
                exec(f'self.checkbutton{v}=Checkbutton(recordFrame,variable=self.check_{v})')
                exec(f'self.checkbutton{v}.deselect()')
                exec(f'self.checkbutton{v}.grid(row=v+2, column=0)')
                self.check_to_append.append(f'self.check_{v}')
      
                
        # Match/Non-Match buttons

        self.match_button = ttk.Button(recordFrame, text='Match', command=lambda: self.update_index(1))
        self.match_button.grid(row=self.len_current_cluster, column=1, columnspan=1, padx=10, pady=10)
        self.non_match_button = ttk.Button(recordFrame, text='No more matches', command=lambda: self.update_index(0))
        self.non_match_button.grid(row=self.len_current_cluster, column=2, columnspan=1, padx=10, pady=10)
        self.back_button=ttk.Button(recordFrame, text='Back', command= lambda: self.go_back())
        self.back_button.grid(row=self.len_current_cluster, column=3, columnspan=1, padx=10, pady=10)
        #self.clear_matches=ttk.Button(recordFrame, text= 'Clear Matches', command=lambda: self.clear_current_cluster())
        #self.clear_matches.grid(row=self.len_current_cluster, column=4, columnspan=1, padx=10, pady=10)
        

        if self.cluster_index==0 and self.current_num_cluster_decisions()==0: 
            self.back_button.config(state= DISABLED)
        
        
        # Add in the comment widget based on config option
        if int(config['custom_settings']['commentbox']):
            
            # Get the position info from button 1
            info_button = self.match_button.grid_info()
            
            self.comment_label = ttk.Label(recordFrame,text='Comment:', font='Helvetica 10 bold')
            self.comment_label.grid(row = info_button['row']+1, column =0, columnspan=1, sticky='e')
            
            self.comment_entry = ttk.Entry(recordFrame)
            self.comment_entry.grid(row = info_button['row']+1, column =1, columnspan=3, sticky='sew', padx=5, pady=5)
            for row in self.display_indexes: 
                self.comment_entry.insert(0, working_file['Comments'][row])
        
        # =====  toolFrame         
        
        # Create labels for tools bar
        self.separator_tf_1 = ttk.Separator(toolFrame, orient='vertical')
        self.separator_tf_1.grid(row=0, column=3, rowspan=1, sticky='ns', padx=10, pady=5)
        self.separator_tf_2 = ttk.Separator(toolFrame, orient='vertical') 
        self.separator_tf_2.grid(row=0, column=7, rowspan=1, sticky='ns', padx=10, pady=5)
        
  
        self.text_smaller_button = Button(toolFrame, text='🗚-', height=1, width=3, command=lambda: self.change_text_size(0))
        self.text_smaller_button.grid(row=0, column=4, sticky='e', pady=5)
        self.text_bigger_button = Button(toolFrame, text='🗚+', height=1, width=3, command=lambda: self.change_text_size(1))
        self.text_bigger_button.grid(row=0, column=5, sticky='w', pady=5, padx=2)  
        # Make text bld button
        self.bold_button = Button(toolFrame,text='B', font='Helvetica 9 bold', height=1, width=3, command=lambda:self.make_text_bold())
        self.bold_button.grid(row=0, column=6, sticky='w', pady=5)
        # Save and close button
        self.save_button = ttk.Button(toolFrame, text='Save and Close 🖫', command=lambda: self.save_and_close())
        self.save_button.grid(row=0, column=8, columnspan=1, sticky='e', padx=5, pady=5)
        
   
    def get_starting_cluster_id(self):
         """
        A function that generates the cluster id number of the first cluster that does not have an answer in the match field. 

        Returns 
        -------
        starting_cluster_id: the sequential cluster id of the first record that has no result in the 'Match' column

        """
         for i in working_file.index: 
             if working_file.loc[i,'Match'] =='': 
                 return working_file.loc[i,'cluster_sequential_number']
             else:
                 pass
    
    def update_gui(self):
        '''
        A simple function that updates the different GUI labels based on the
        records. 
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None.

        '''
        self.counter_matches.config(text=f'{self.cluster_index+1} / {self.num_clusters} Clusters', font=f'Helvetica {self.text_size}')
       # self.datasource_label.config(font=f'Helvetica {self.text_size} bold')
        
        for non_iter_columns in self.non_iterated_labels:
            
            exec(f'self.{non_iter_columns}.config(font=f"Helvetica {self.text_size} bold")')
                       
        
        display_indexes = working_file.index[working_file['cluster_sequential_number']==self.cluster_index].to_list()
        
             
        if int(config['custom_settings']['commentbox']):
            self.comment_entry.delete(0, END)
            for i in self.display_indexes:
                self.comment_entry.insert(0, working_file['Comments'][i])
  
        #update the text widgets and deselect the checkbuttons
        for n, iter_columns in enumerate(config.options('columnfile_info_and_order')):
            col_header = config['columnfile_info_and_order'][iter_columns].replace(' ','').split(',')
            for v, display_i in enumerate(display_indexes):
            
                exec(f'self.{col_header[0]}row{v}.config(state=NORMAL)')
            
                exec(f'self.{col_header[0]}row{v}.delete("1.0","end")')
            
                exec(f'self.{col_header[0]}row{v}.insert("1.0",working_file["{col_header[0]}"][{display_i}])')
            
                exec(f'self.{col_header[0]}row{v}.config(width = len(working_file["{col_header[0]}"][{display_i}])+10, font=f"Helvetica {self.text_size} {self.text_bold}", state=DISABLED)')
            
                exec(f'self.checkbutton{v}.deselect()')

                if working_file.loc[display_i, 'Match'] == "":
                    
                    exec(f'self.checkbutton{v}.config(state=NORMAL)')
                    
                else:
                                        
                    exec(f'self.checkbutton{v}.config(state=DISABLED)')
   

        if self.cluster_index==0 and self.current_num_cluster_decisions()==0: 
            self.back_button.config(state= DISABLED)
        else:
            self.back_button.config(state= 'normal')
                
    def get_matches(self):
        """
        A function that generates a string based on the matches identified in a cluster. 

        Returns
        -------
        None.

        """
        #create, as a local variable, the list of matches within the current cluster
        list_of_matches=[]
       
        #add to the list of matches; the index of any that are selected by the checkbox.
        for v, display_i in enumerate(self.display_indexes):

                status=eval(f'self.check_{v}.get()')            
                if status == True:
                    list_of_matches.append(display_i)
                    
        #creates a string that is the record id's that match (column indicated in config file); separated by a comma
        #assign string to global variable self.match_string
        for i in list_of_matches: 
            temp_string=str(working_file[config['record_id_col']['record_id']][i]) + ','
            self.match_string = self.match_string + temp_string
            
        
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
        current_cluster_decisions = [working_file.loc[i, 'Match'] for i in self.display_indexes]
        
        # counting records as a match if there's something in their corresponding match column
        current_num_cluster_decisions = len([record for record in current_cluster_decisions if record != ""])
        
        return current_num_cluster_decisions
        
      
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
                    
                    messagebox.showwarning(message = "Two or more records must be selected to make a match")
                    
                    break
                
                # otherwise if the match column is currently empty
                elif working_file.loc[i, 'Match'] == "":
                    
                    # if a record is selected by checkbutton
                    if (working_file[config['record_id_col']['record_id']][i]) in (self.match_string):
                        
                        # append currently selected records' record ids to the match column
                        working_file.loc[i, 'Match'] = self.match_string
                     
                        # remove matched records in cluster from list of those not yet matched
                        try:
                            self.not_matched_yet.remove(i)
                        
                        # for cases where the matched records have already been removed (big clusters w/ lots of decisions)
                        except ValueError:
                            
                            pass
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
 
        if int(config['custom_settings']['commentbox']):
            for a in self.display_indexes:
                working_file.at[a, 'Comments'] = self.comment_entry.get()
                
    

    def go_back(self): 
        '''
        A function that goes back to the previous record. 

        Returns
        -------
        None.

        '''
        #get the number of decisions made in current cluster        
        num_decisions= self.current_num_cluster_decisions()
        
        #update cluster_index IF there are no descisions in current cluster
        if num_decisions ==0: 
            self.cluster_index-=1
            self.display_indexes=working_file.index[working_file['cluster_sequential_number']==self.cluster_index].to_list()
        
        #reset new (previous record) to empty strings
        for i in self.display_indexes:
            working_file.loc[i, 'Match'] =""
            working_file.loc[i, 'Comments']=""
            
        #clean the match string
        self.match_string=''
        
        #clear list of not matched yet
        self.not_matched_yet.clear()
        
        #spdate the gui 
        self.update_gui()

        
        #configure match_buttons to normal 
        self.match_button.config(state = 'normal')
        self.non_match_button.config(state  = 'normal')
        
        
        
        
        
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
        # Query whether the current record matches the total number of records (end of the terminal)
        if self.cluster_index > (self.num_clusters-1): 
            # disable the match and Non-match buttons
            self.match_button.configure(state=DISABLED)
            self.non_match_button.configure(state=DISABLED)
            # present a message on the screen infomring the user that matching is finished
            self.matchdone = ttk.Label(root, text='Matching Finished. Press save and close.',foreground='red')
            self.matchdone.grid(row=1, column=0, columnspan=1)
            
            return 1
        else: 
            
            return 0
        
            
    def save_and_close(self):
        '''
        This function saves the working_file dataframe and closes the GUI

        Parameters
        ----------
        filepath : string type
            This should be the exact directory that will be saved.

        Returns
        -------
        None.

        '''
        # Check whether matching has now finished (i.e. they have completed all records)
        if self.cluster_index == (self.num_clusters): 
            # if matching is now complete rename the file 
         os.rename(self.filename_old, self.filename_done)
         working_file.to_csv(self.filename_done, index=False)


        else:
            # If not it yet finshed save it using the old file name
            working_file.to_csv(self.filename_old, index=False)
        
        # close down the app
        root.destroy()
        
    
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
                     
        #Update the list of matching record IDs
        self.get_matches()
       
        #update the underlying dataframe with matching record IDs
        self.update_df(event)
        
            
        # if all records in cluster haven't had a matching decision made on them yet AND the match button has been clicked
        if len(self.display_indexes) >  self.current_num_cluster_decisions() and event == 1:
        
            #reset match string so different pairings can be made in that cluster
            self.match_string = ''

            # Update the GUI labels
            self.update_gui()
                        
            # clear the list of records in cluster remaining unmatched
            self.not_matched_yet.clear()
            
        
        else:
        
            #reset match string so different pairings can be made in that cluster
            self.match_string = ''

            # Update the GUI labels
            self.update_gui()
                        
            # clear the list of records in cluster remaining unmatched
            self.not_matched_yet.clear()
            
            #update the cluster_index and display indexes to referece the new cluster
            self.cluster_index +=1
            self.display_indexes= working_file.index[working_file['cluster_sequential_number']==self.cluster_index].to_list()
            self.len_current_cluster= len(self.display_indexes)
            
            #reset match string
            self.match_string = ''
            
            stp_gui = self.check_matching_done()
            
            # Check if reached the end of the script
            if stp_gui:
                pass
                # could add in additional functionality here to do with saving the working_file file
            else: 
                # Update the GUI labels
                self.update_gui()
        

    def change_text_size(self,size_change):
        '''
        This function will increase or decrease the size of the text. It then 
        updates the GUI.
        It also changes the size of the window to fit the text.

        Parameters
        ----------
        size_change : int - boolean
            Will change the text size based on argument passed. 

        Returns
        -------
        None.

        '''
        # depending on the argument passed - increase or decrease the text size/geometry paramaters
        if size_change:
            self.text_size += 1

        else:
            self.text_size -= 1

        # update the gui
        self.update_gui()
        
    def on_exit(self):
        '''
        When you click to exit, this function is called, which creates a message
        box that questions whether the user wants to Exit without saving 
        
        '''
        # if they click yes
        if messagebox.askyesno("Exit", "Are you sure you want to exit WITHOUT saving?"):

            # check if this is the first time they are accessing it 
            if (self.matching_previously_began == False) & (self.checkpointcounter == 0): 
                # then rename the file removing their intial and 'inProgress' tag
                os.rename(self.filename_old,'_'.join(self.filename_old.split('_')[0:-2]) + '.csv')
            
            # close down the application
            root.destroy()

if __name__ == "__main__":
    
    # ------
    # Step 1: 
    # Load config file and get the file directory
    # Get user credentials
    # Open intor window and get user to choose file
    # ------ 
    
    # Import the configs for the project 
    config = configparser.ConfigParser()
    config.read('Config_clusters.ini')
    
    # Get the initial directory folder
    initdir = config['matching_files_details']['file_pathway']
    
    # specify file types - this will only show these files when the dialog box opens up
    filetypes = (('csv files', '*.csv'), )
    
    # grab user credentials
    user = getpass.getuser()
    
    # ===================== Open Intro GUI 
    # Open a file pen dialog box, allow user to choose file, then grab user credentials  
    root = Tk()
    # Run the Intro GUI
    intro = IntroWindow(root, initdir, filetypes)

    root.mainloop()    

    # END OF STEP 1

    # ------
    # Step 2:
    # ------ 
    
    # ------- Create filepath variables, load in the selected data and specify column variables
    
    # Check if the user running it has selected this file before (this means they have done some of the matching already and are coming back to it)
    if 'inProgress' in intro.fileselect.split('/')[-1]:
        
        # If it is the same user
        if (user in intro.fileselect.split('/')[-1]):
        
            # Dont rename the file 
            renamed_file =  intro.fileselect
            
            # create the filepath name for when the file is finished
            filepath_done = f"{'/'.join(renamed_file.split('/')[:-1])}/{renamed_file.split('/')[-1][0:-15]}_DONE.{renamed_file.split('/')[-1].split('.')[-1]}"
            
        else:

            # Rename the file to contain the additional user
            renamed_file =  f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1].split('.')[0][0:-11]}_{user}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"           
            os.rename(rf'{intro.fileselect}',rf'{renamed_file}')
            
            # create the filepath name for when the file is finished
            filepath_done = f"{'/'.join(renamed_file.split('/')[:-1])}/{renamed_file.split('/')[-1][0:-15]}_DONE.{renamed_file.split('/')[-1].split('.')[-1]}"
                        
    # If a user is picking this file again and its done
    elif 'DONE' in intro.fileselect.split('/')[-1]:
        
        # If it is the same user
        if (user in intro.fileselect.split('/')[-1]):
            # dont change filepath done - keep it as it is
            filepath_done = intro.fileselect
        
            # Rename the file 
            renamed_file =  f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1][0:-9]}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"
            os.rename(rf'{intro.fileselect}',rf'{renamed_file}')
        else:
            # If it is a different user
            # Rename the file to include the additional user
            renamed_file =  f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1].split('.')[0][0:-5]}_{user}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"
            os.rename(rf'{intro.fileselect}',rf'{renamed_file}')
            
            # create the filepath done
            filepath_done = f"{'/'.join(renamed_file.split('/')[:-1])}/{renamed_file.split('/')[-1][0:-15]}_DONE.{renamed_file.split('/')[-1].split('.')[-1]}"
            
            
    else:
        # Resave this file with the user ID at the end so no one else selects it 
        # rename it with '_inProgress' and their entered initials
        renamed_file = f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1].split('.')[0]}_{user}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"
        os.rename(rf'{intro.fileselect}',rf'{renamed_file}')
        
        # create the filepath name for when the file is finished
        filepath_done = f"{'/'.join(renamed_file.split('/')[:-1])}/{renamed_file.split('/')[-1][0:-15]}_DONE.{renamed_file.split('/')[-1].split('.')[-1]}"

        
    # ---- load in the required csv file as a pandas dataframe (can also do this for excel docs...)
    working_file = pd.read_csv(renamed_file)
                                    
    # END OF STEP 2
                                    
    # ------
    # Step 3:
    # ------ 
    
    
    # ------ Run the Clerical Matching Application
    

    # ---- run the clerical matching app
    root = Tk()
    mainWindow = ClericalApp(root, working_file, filepath_done, renamed_file, config)
    root.mainloop()

    print("\n Number of records matched:", str(len(working_file[(working_file.Match != 'No match in cluster') & (working_file.Match != "")])))
