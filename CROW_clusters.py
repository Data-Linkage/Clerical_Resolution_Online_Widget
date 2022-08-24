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

Anthony G Edwards -- Anthony.G.Edwards@ons.gov.uk -- Co-Lead Developer

Craig Scott -- Craig.Scott@ons.gov.uk -- Creator 
You can also contact the linkage hub at: 
    
linkage.hub@ons.gov.uk
_________
We would like to acknowledge and thank David Cobbledick and Andrew Sutton for
reviewing this code.
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
        root.eval("tk::PlaceWindow . center")

        # initialise some variables - what are these used for?
        self.init_dir = init_dir 
        self.files_info = files_info 
        
        # initialise the frame 
        self.content = ttk.Frame(root) 
        self.frame = ttk.Frame(self.content, borderwidth=5, relief='ridge',
                               width=500, height=300)
        self.content.grid(column=0, row=0)
        self.frame.grid(column=0, row=0, columnspan=5, rowspan=5)
        
        # create some widgets and place them on the gui
        self.intro_text = ttk.Label(self.content, text=('Welcome to the Clerical Matching Application. \nPlease click "Choose File" to select your file \nand begin matching.'),font='Helvetica 10')
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
        root.eval("tk::PlaceWindow . center")
        
        # Create the separate frames 
        # 1 - Tool Frame 
        self.toolFrame = ttk.LabelFrame(root, text='Tools:')
        self.toolFrame.grid(row=0, column=0, columnspan=1, sticky='ew',
                            padx=10, pady=10)
        
        # 2 - Record Frame
        self.recordFrame = ttk.LabelFrame(root, text='Current Record')
        self.recordFrame.grid(row=1, column=0, columnspan=1, padx=10, pady=3)
 
        # 2 - Button Frame
        self.buttonFrame = ttk.LabelFrame(root)
        self.buttonFrame.grid(row=2, column=0, columnspan=1, padx=10, pady=3)
        
        
        # initalise the file name variables
        self.filename_done = filename_done
        self.filename_old = filename_old
        
        # list of record IDs that have not been matched yet
        self.not_matched_yet = []
        
        # create protocol for if user presses the 'X' (top right) 
        root.protocol("WM_DELETE_WINDOW", self.on_exit)
    
        # if match column exists in clerical file
        if {'Match'}.issubset(working_file.columns):
            
            # variable indicates whether user has returned to this file (1) or not (0)
            self.matching_previously_began = 1
        
        else:
            
            # create a match column and fill with blanks
            working_file['Match'] = ''
            
            self.matching_previously_began = 0
            
        # convert all columns apart from Match and Comments (if specified) to string
        for col_header in working_file.columns:
            
            if (col_header == 'Match') or (col_header == 'Comments'):
                pass
            else:
                # convert to string
                working_file[col_header] = working_file[col_header].astype(str)
                # remove nan values 
                for i in range(len(working_file)):
                    
                    # should this be 'NaN' after casting to str?
                    if working_file[col_header][i] == 'nan':
                        
                        working_file.at[i,col_header] = ''
                        
        working_file.fillna('', inplace=True)
        
        #a counter of the number of checkpoint saves. 
        self.checkpointcounter = 0
        
        # create a sequential cluster id number from the cluster id variable
        cluster_var=config['cluster_id_number']['cluster_id']
        working_file['cluster_sequential_number'] = pd.factorize(working_file[cluster_var])[0]
        
        # list of cluster numbers over which to iterate
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
        
        self.s = ttk.Style()
        self.s.configure('.', font= ('Helvetica', f'{self.text_size}'))
        
        # SHOW/HIDE DIFFERENCES CLASS VARAIBLES
        #toggle on and off
        self.show_hide_diff = 0
        #a container to hold the names of all their tags
        self.tags_container = {}
        #a dicstionary containing column headers as keys and corresponding items of the first row as values
        self.comparison_values={}
        #A list of all the columns that need comparing. 
        self.columns_to_compare=[]
       
        # Create empty lists of labels 
        self.non_iterated_labels = []
        self.iterated_labels = []
        
       
        # ---------------------
        # Create dataset name widgets and separators between records
        row_adder = 0
        separator_adder = 2
        
       # self.style=ttk,Style(self)
        # ---------------------
  
        self.draw_recordframe(config, working_file)
        self.draw_buttonFrame()
        self.draw_toolFrame()   

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
    
    def draw_buttonFrame(self):
        # =====  buttonFrame - for match/non-match/back buttons        

        self.match_button = Button(self.buttonFrame, text='Match', font=f'Helvetica {self.text_size}', command=lambda: self.update_index(1), bg='DarkSeaGreen1')
        self.match_button.grid(row=self.len_current_cluster, column=1, columnspan=1, padx=10, pady=10)
        self.non_match_button = Button(self.buttonFrame, text='No more matches', font=f'Helvetica {self.text_size}', command=lambda: self.update_index(0),bg='light salmon')
        self.non_match_button.grid(row=self.len_current_cluster, column=2, columnspan=1, padx=10, pady=10)
        self.back_button=Button(self.buttonFrame, text='Back', font=f'Helvetica {self.text_size}', command= lambda: self.go_back())
        self.back_button.grid(row=self.len_current_cluster, column=3, columnspan=1, padx=10, pady=10)   

        # disable back button if no previous clusters exist
        if self.cluster_index==0 and self.current_num_cluster_decisions()==0: 
            self.back_button.config(state= DISABLED)
        
        # Add in the comment widget based on config option
        if int(config['custom_settings']['commentbox']):
            
            # create comments column if one doesn't exist
            if 'Comments' not in working_file:
                working_file['Comments'] = ''

            # Get the position info from button 1
            info_button = self.match_button.grid_info()
            
            self.comment_label = ttk.Label(self.buttonFrame,text='Comment:', font=f'Helvetica {self.text_size} bold')
            self.comment_label.grid(row = info_button['row']+1, column =0, columnspan=1, sticky='e')
            
            self.comment_entry = ttk.Combobox(self.buttonFrame)
            self.comment_entry.grid(row = info_button['row']+1, column =1, columnspan=3, sticky='sew', padx=5, pady=5)
            
            if (config['custom_settings']['comment_values']) is not None:
                self.comment_entry['values'] = (config['custom_settings']['comment_values']).split(",")

                
    def draw_toolFrame(self):
        # =====  toolFrame         
        

        self.text_smaller_button = Button(self.toolFrame, font=f'Helvetica {self.text_size}', text='ᴀA-', height=1, width=3, command=lambda: self.change_text_size(0))
        self.text_smaller_button.pack(side=LEFT, padx=5)

        self.text_bigger_button = Button(self.toolFrame, font=f'Helvetica {self.text_size}', text='ᴀA+', height=1, width=3, command=lambda: self.change_text_size(1))
        self.text_bigger_button.pack(side= LEFT, padx=5)  

        
        # Make text bld button
        self.bold_button = Button(self.toolFrame,text='B', font=f'Helvetica {self.text_size} bold', height=1, width=3, command=lambda:self.make_text_bold(config, working_file))
        self.bold_button.pack(side=LEFT, padx=5)
 
        
        # Save and close button
        self.save_button = Button(self.toolFrame, text='Save and Close 🖫', font=f'Helvetica {self.text_size}', command=lambda: self.save_and_close())
        self.save_button.pack(side=RIGHT, padx=5)
  
        
        # highlighter
        self.highlighter_button= Checkbutton(self.toolFrame, indicatoron = 0, selectcolor = "white", text= 'show/hide differences', font=f'Helvetica {self.text_size}', command= lambda: self.show_hide_differences(self.show_hide_diff))
        self.highlighter_button.pack(side=LEFT, padx=5)


    def draw_recordframe(self, config, working_file):

        
        # try to calculate and display remaining # clusters for matching
        try:
            self.counter_matches = ttk.Label(self.recordFrame, text=f'{self.cluster_index+1} / {self.num_clusters} Clusters', font=f'Helvetica {self.text_size}')
            self.counter_matches.grid(row=0, column=len(config.options('column_headers_and_order')), columnspan=1, padx=10, sticky="e") 

        # except when matching is completed
        except TypeError: 
            
            messagebox.showinfo(title = "Matching completed", message = "Please select a different file to clerically match")
            
            # close down the application
            root.destroy()
        num_match_cols=0
        # Create column header labels and place all them on row 1, column n+2 (where n == the enumerate of the list)
        for n, column_title in enumerate(config.options('column_headers_and_order')):
            
            # Remove spaces from the user input and split them into different components
    
            col_header = config['column_headers_and_order'][column_title].replace(' ','').split(',')
            
            exec(f'self.{column_title} = ttk.Label(self.recordFrame,text="{col_header[0]}",font=f"Helvetica {self.text_size} bold")')
    
            exec(f'self.{column_title}.grid(row=1,column=n+1,columnspan=1, sticky = W, padx=10, pady=3)')
            
            # Add the executed self.labels for the column headers to the non_iterated_labels list
            self.non_iterated_labels.append(column_title)
            num_match_cols+=1
              
        #iterate over column info and order. 
        for n, columnfile_title in enumerate(config.options('columnfile_info_and_order')):
            
            row_num=3
            sep_row=4
            
            #create a style for header separator
            styl = ttk.Style()
            styl.configure('grey.TSeparator', background='Wheat4')
            header_separator=ttk.Separator(self.recordFrame,  orient='horizontal', styl='grey.TSeparator')
            if self.text_size !=10:
                text_size_multiplier=(1+((self.text_size-10)/10))
        
            elif self.text_size ==10: 
                text_size_multiplier=1
         
                #grid separator
            header_separator.grid(row=2, column=0, columnspan=num_match_cols+1, sticky='ns', ipadx=80*(num_match_cols+1)*text_size_multiplier, ipady=1)
            
            for v, display_i in enumerate(self.display_indexes):
            
                col_header = config['columnfile_info_and_order'][columnfile_title].replace(' ','').split(',')
            
                # create a text label
                exec(f'self.{col_header[0]}row{v} = Text(self.recordFrame,height=1,relief="flat",bg="gray93")')
                
                # Enter in the text from the df
                exec(f'self.{col_header[0]}row{v}.insert("1.0",working_file["{col_header[0]}"][{display_i}])')
                
                # configure Text so that it is a specified width, font and cant be interacted with
                exec(f'self.{col_header[0]}row{v}.config(width=len(working_file["{col_header[0]}"][{display_i}])+10,font=f"Helvetica {self.text_size} {self.text_bold}",state=DISABLED)')
                
                #grid the text label to the widget. 
                exec(f'self.{col_header[0]}row{v}.grid(row={row_num}, column={n+1},columnspan=1,padx=10, pady=3,sticky="w")')
                
                #create a checkbutton and append it to the list of checkbutton variables. 
                exec(f'self.check_{v}= IntVar()')
                exec(f'self.checkbutton{v}=Checkbutton(self.recordFrame,variable=self.check_{v})')
                exec(f'self.checkbutton{v}.deselect()')
                exec(f'self.checkbutton{v}.grid(row={row_num}, column=0)')

                exec(f"rf_separator{v}=ttk.Separator(self.recordFrame,  orient='horizontal')")
                exec(f"rf_separator{v}.grid(row={sep_row}, column=0, columnspan={num_match_cols}+1, sticky='ns', ipadx=80*({num_match_cols+1})*{text_size_multiplier}, ipady=1)")
                
                if col_header[0] not in self.columns_to_compare:
                    self.columns_to_compare.append(col_header[0])
                
                # if match column not populated yet, keep checkbutton clickable
                if working_file.loc[display_i, 'Match'] == "":
                    exec(f'self.checkbutton{v}.config(state=NORMAL)')
               
                # else make it unclickable
                else:
                    exec(f'self.checkbutton{v}.config(state=DISABLED)')
                    
                row_num+=2
                sep_row+=2

                    
    def update_gui(self, config, working_file):
        '''
        A simple function that updates the different GUI labels based on the
        records. This function is called whenever the app is interacted with,
        i.e. when pressing match/non-match/back buttons.
        
        Parameters
        ----------
        None
        
        Returns
        -------
        None.

        '''
        
        # # if commentbox specified in config

        # clear recordframe
        for widget in self.recordFrame.winfo_children():
            
            widget.destroy()
        
        for widget in self.toolFrame.winfo_children():
            
            widget.destroy()
                        
        for widget in self.buttonFrame.winfo_children():
            
            widget.destroy()
            
        # redraw everything in recordFrame
        self.draw_recordframe(config, working_file)       
        self.draw_buttonFrame()
        self.draw_toolFrame()
       
        # clear commentbox entry
        if int(config['custom_settings']['commentbox']):
            self.comment_entry.delete(0, END)
            
        # disable back button if no previous clusters exist
        if self.cluster_index==0 and self.current_num_cluster_decisions()==0: 
            self.back_button.config(state= DISABLED)
        else:
            self.back_button.config(state= 'normal')
        
        self.tags_container = {}
        self.comparison_values={}
        
        if self.show_hide_diff==1:
            self.show_hide_differences(0)
    
    def make_text_bold(self, config, working_file):   
        '''
        Makes the text bold or not

        Returns
        -------
        None.

        '''
        if not self.text_bold_boolean:
            self.text_bold_boolean = 1
            self.text_bold = 'bold'

        else:
            self.text_bold_boolean = 0
            self.text_bold = ''
        
        # update the gui
        self.update_gui(config, working_file)
                  
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
                        
                        # if commentbox specified in config
                        if int(config['custom_settings']['commentbox']):
                            
                            # at each row index of the checkbox selected, append the contents of the comment box
                            working_file.loc[i, 'Comments'] = self.comment_entry.get()

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
                    
                            working_file.loc[i, 'Comments'] = self.comment_entry.get()
                           

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
        self.update_gui(config, working_file)

        
        #configure match_buttons to normal 
        self.match_button.config(state = 'normal')
        self.non_match_button.config(state  = 'normal')
        
        # this is to handle exceptions encountered when self.matchdone is not defined, i.e. if user presses back button on the first cluster in the data
        try: 
            self.matchdone.destroy()
        except AttributeError: 
            pass
        
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
                                                                                         
    def show_hide_differences(self, toggle):
        """
        

        Parameters
        ----------
        toggle : TYPE
            A variable to indicate if the show-hide-differences is already on. 

        Returns
        -------
        None.

        """
        if toggle == 0:
            
            # make show show diff variable 1 so that next time this function is 
            # called it will remove tags
            self.show_hide_diff = 1
            
            #For the first row in the cluster: for each column to compare; 
            #add col and value to self.comparisso_values
            for col in self.columns_to_compare:
                self.comparison_values[col]=working_file.loc[self.display_indexes[0],col]
                
                #create a dictionary for the current comparison
                current_comparison={}
                
                #for each comparison row
                for n,current_comparison_row in enumerate(self.display_indexes[1:]):
                    
                    #create column:value pair 
                    current_comparison[col]=working_file.loc[current_comparison_row, col]
                         # For the values in datarows that need to be highlighted 
              
                    # some empty variables to control the flow of the difference indicator
                    #a list of list to hold start and end of difference value:
                    char_consistent = []
                    
                    #a list to hold the start and end value of differences for the current itteration:
                    container = []
                    string_start = 1
                    string_end = 0
                    count= 0
                    
                    #zip together comparison values and current comparison and compare each zipped item
                    for char_comparison, char_highlight in zip(self.comparison_values[col],current_comparison[col]):
                        
                        # if the comparison char is not the same as the highlighter char
                        if char_comparison != char_highlight:
                            
                            # if this is the first diff append count to container
                            if string_start: 
                                # start the container values
                                container.append(count)
                                
                                string_start = 0
                            
                            # if we are at the end of string comparison    
                            if count == min(len(self.comparison_values[col]),len(current_comparison[col]))-1: 
                                    
                                container.append(count+1)
                                # pass this start and end values to the overall container    
                                char_consistent.append(container)
                                
                        elif char_comparison==char_highlight: 
                            if string_end == string_start:
                                # add it to the container to complete the char number 
                                #differences
                                container.append(count)
                                
                                #restart this variable
                                string_start = 1
                                
                                # pass this start and end values to the overall container                                    
                                char_consistent.append(container)
                                
                                container = []
                        #increase the count
                        count += 1
                    
                        
                          
                    # for each tag number in char consistent create the tag and save the tag name information 
                    for tag_adder in range(len(char_consistent)):
                        
                            if col in self.tags_container:
                                
                                temp_val=f'{col}_diff{str(tag_adder)}'
                                if temp_val not in self.tags_container[col]:
                                    self.tags_container[col].append(f'{col}_diff{str(tag_adder)}')
                            
                            else:
                 
                                self.tags_container[col] = [f'{col}_diff{str(tag_adder)}']
        
                            exec(f'self.{col}row{n+1}.tag_add(f"{col}_diff{str(tag_adder)}",f"1.{char_consistent[tag_adder][0]}", f"1.{char_consistent[tag_adder][-1]}")')
                               
                            exec(f'self.{col}row{n+1}.tag_config(f"{col}_diff{str(tag_adder)}",background="yellow",foreground = "black")')

        else:
             # reset this variable
             self.show_hide_diff = 1
             
             # for all variable labels with differences - remove the tag labels
             for n in range(0,len(self.display_indexes)-1):
                 
                 #for columns in self.columns_to_compare:    
                for col, value in self.tags_container.items():
                    for item in value:
                        exec(f"self.{col}row{n+1}.tag_remove('{item}','1.0','end')")
                      
             self.show_hide_diff=0
            
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
        
        # Update the GUI labels
        self.update_gui(config, working_file)

        #reset match string so different pairings can be made in that cluster
        self.match_string = ''
        
        # clear the list of records in cluster remaining unmatched
        self.not_matched_yet.clear()
            
        # if all records in cluster haven't had a matching decision made on them yet and the match button has been clicked
        # i.e. not the non-match button, so pairings can still be made within current cluster
        if len(self.display_indexes) >  self.current_num_cluster_decisions() and event == 1:
            pass
    
        # if no more pairs can be made in cluster after clicking match OR no more matches button is clicked
        else:
            
            #update the cluster_index and display indexes to referece the new cluster
            self.cluster_index +=1
            self.display_indexes= working_file.index[working_file['cluster_sequential_number']==self.cluster_index].to_list()
            self.len_current_cluster= len(self.display_indexes)
            
            stp_gui = self.check_matching_done()
            self.tags_container = {}
        
            # Check if reached the end of the script
            if stp_gui:
                pass
                # could add in additional functionality here to do with saving the working_file file
            else: 
                # Update the GUI
                self.update_gui(config, working_file)

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
        self.update_gui(config, working_file)
        
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

    root = Tk()
    mainWindow = ClericalApp(root, working_file, filepath_done, renamed_file, config)
    root.mainloop()

    print("\n Number of records matched:", str(len(working_file[(working_file.Match != 'No match in cluster') & (working_file.Match != "")])))
