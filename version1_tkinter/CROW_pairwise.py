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
Craig Scott -- Craig.Scott@ons.gov.uk -- Creator and Lead Developer
Hannah O'Dair -- Hannah.O'Dair@ons.gov.uk -- Co-Lead Developer
_________
We would like to acknowledge and thank David Cobbledick and Andrew Sutton for reviewing this code.
"""

import configparser
import getpass
import os
import tkinter
from tkinter import filedialog, ttk

import pandas as pd

########## Intro GUI


class IntroWindow:
    """
    intro_window class function - opens a window that prompts the user to
    choose their matching file.

    """

    def __init__(self, root, init_dir, files_info):
        # Initialise the gui paramaters
        root.geometry("400x225")
        root.title("Clerical Matching")

        # initialise some variables
        self.init_dir = init_dir
        self.files_info = files_info

        # initialise the frame
        self.content = ttk.Frame(root)
        self.frame = ttk.Frame(
            self.content, borderwidth=5, relief="ridge", width=500, height=300
        )
        self.content.grid(column=0, row=0)
        self.frame.grid(column=0, row=0, columnspan=5, rowspan=5)

        # create some widgets and place them on the gui
        self.intro_text = ttk.Label(
            self.content,
            text='Welcome to the Clerical Matching Application. \nPlease click "Choose File" to select your file \nand begin matching.',
            font="Helvetica 10",
        )
        self.intro_text.grid(row=1, column=0, columnspan=4)

        # create the button
        self.choose_file_button = ttk.Button(
            self.content, text="Choose File", command=lambda: self.open_dirfinder()
        )
        self.choose_file_button.grid(row=2, column=1, columnspan=1, sticky="new")

    def open_dirfinder(self):
        """
        Opens a file select window, allows user to choose a file. Ends the GUI.
        Returns
        -------
        None.
        """
        # Open up a window that allows the user to choose a matching file
        self.fileselect = filedialog.askopenfilename(
            initialdir=self.init_dir,
            title="Please select a file:",
            filetypes=self.files_info,
        )

        # close down intro_window
        root.destroy()


class ClericalApp:
    """
    ClericalApp class function - opens a window and allows users to clerically review records.

    """

    def __init__(self, root, working_file, filename_done, filename_old, config):
        """Initialise the ClericalApp class."""
        # Initialise some parameters.
        self.root = root
        self.filename_done = filename_done
        self.filename_old = filename_old

        # Set the window title.
        root.title("Clerical Matching")

        # Set the window size to 90% of screen width and 50% of screen
        # height.
        width = int(self.root.winfo_screenwidth() * 0.9)
        height = int(self.root.winfo_screenheight() * 0.5)
        self.root.geometry(f"{width}x{height}")

        # Configure the grid layout to make sure the record frame can
        # expand to fill the window.
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(1, weight=1)

        # Create the separate frames
        # 1 - Tool Frame
        self.toolFrame = ttk.LabelFrame(root, text="Tools:")
        self.toolFrame.grid(
            row=0, column=0, columnspan=1, sticky="ew", padx=10, pady=10
        )

        # 2 - Record Frame: Create a container to hold the scrollable
        # canvas that will contain the record frame.
        self.record_container = ttk.Labelframe(root, text="Records")
        self.record_container.grid(row=1, column=0, padx=10, sticky="nsew")

        # Create the canvas and scrollbars. Attach the scrollbar
        # functionality to the canvas.
        self.canvas = tkinter.Canvas(self.record_container)
        self.vertical_scrollbar = ttk.Scrollbar(
            self.record_container, orient="vertical", command=self.canvas.yview
        )
        self.horizontal_scrollbar = ttk.Scrollbar(
            self.record_container, orient="horizontal", command=self.canvas.xview
        )
        self.canvas.configure(
            yscrollcommand=self.vertical_scrollbar.set,
            xscrollcommand=self.horizontal_scrollbar.set,
        )

        # Add the scrollbars and canvas to the window.
        self.vertical_scrollbar.pack(side="right", fill="y")
        self.horizontal_scrollbar.pack(side="bottom", fill="x")
        self.canvas.pack(side="left", fill="both", expand=True)

        # Create the record frame.Place the record frame in the canvas.
        self.record_frame = ttk.Frame(self.canvas)
        self.canvas.create_window((0, 0), window=self.record_frame, anchor="nw")

        # Make sure the scrollbars update whenever the record frame
        # grows beyond the viewport.
        self.record_frame.bind("<Configure>", self._on_record_frame_configure)

        # Bind the mousewheel events for vertical and horizontal
        # scrolling.
        self.canvas.bind_all(
            "<MouseWheel>",
            lambda e: self.canvas.yview_scroll(int(-1 * (e.delta / 120)), "units"),
        )
        self.canvas.bind_all(
            "<Shift-MouseWheel>",
            lambda e: self.canvas.xview_scroll(int(-1 * (e.delta / 120)), "units"),
        )

        # 3 -Button Frame
        self.button_frame = ttk.Frame(root)
        self.button_frame.grid(row=2, column=0, columnspan=1, padx=10, pady=10)

        # create protocol for if user presses the 'X' (top right)
        root.protocol("WM_DELETE_WINDOW", self.on_exit)

        # create a match column if one doesn't exist
        # replace any missing values (NA) with blank spaces
        if {"Match"}.issubset(working_file.columns):
            # convert all columns apart from Match and Comments (if specified) to string
            for col_header in working_file.columns:
                if col_header in ("Match", "Comments"):
                    pass
                else:
                    # convert to string
                    working_file[col_header] = working_file[col_header].astype(str)
                    # remove nan values
                    for i in range(len(working_file)):
                        if working_file[col_header][i] == "nan":
                            working_file.at[i, col_header] = ""

            working_file.fillna("", inplace=True)

            # variable indicates whether user has returned to this file or not
            self.matching_previously_began = 1
        else:
            working_file["Match"] = ""
            # Create the comment box column if column header is specified
            if int(config["custom_settings"]["commentbox"]):
                working_file["Comments"] = ""

            # convert all columns apart from Match and Comments (if specified) to string
            for col_header in working_file.columns:
                if col_header in ("Match", "Comments"):
                    pass
                else:
                    # convert to string
                    working_file[col_header] = working_file[col_header].astype(str)
                    # remove nan values
                    for i in range(len(working_file)):
                        if working_file[col_header][i] == "nan":
                            working_file.at[i, col_header] = ""

            working_file.fillna("", inplace=True)

            self.matching_previously_began = 0

        # count how many records are in the CM file
        self.num_records = len(working_file)

        # a counter of the number of checkpoint saves.
        self.checkpointcounter = 0

        # initiate start record_index so that it will go from latest record
        # counter variable for iterating through the CM file
        self.record_index = self.get_starting_index()

        self.records_per_checkpoint = int(
            config["custom_settings"]["num_records_checkpoint"]
        )

        # create the text size component
        self.text_size = 10
        self.text_bold_boolean = 0
        self.text_bold = ""

        # show or hide differences boolean
        self.show_hide_diff = 0
        self.difference_col_label_names = {}

        # Create empty lists of labels
        self.non_iterated_labels = []
        self.iterated_labels = []

        self.draw_button_frame()
        self.draw_record_frame()
        self.draw_tool_frame()
        # ---------------------
        # Create dataset name widgets and separators between

    def _on_record_frame_configure(self, event: tkinter.Event) -> None:
        """Ensure scrollregion is as tall as the canvas."""
        x1, y1, x2, y2 = self.canvas.bbox("all")
        canvas_height = self.canvas.winfo_height()
        bottom = max(y2, canvas_height)
        self.canvas.configure(scrollregion=(x1, y1, x2, bottom))

    def draw_record_frame(self):
        row_adder = 0
        separator_adder = 2

        for iterator, name_of_dataset in enumerate(config.options("dataset_names")):
            exec(
                f'self.{name_of_dataset} = ttk.Label(self.record_frame,text=config["dataset_names"]["{name_of_dataset}"]+":",font=f"Helvetica {self.text_size} bold")'
            )

            exec(
                f'self.{name_of_dataset}.grid(row=3+{row_adder}, column=0, columnspan=1, padx=10, pady=3, sticky = "w")'
            )

            exec(
                f'self.separator{iterator} = ttk.Separator(self.record_frame,orient="horizontal")'
            )

            exec(
                f'self.separator{iterator}.grid(row={separator_adder},column=0,columnspan=len(config.options("column_headers_and_order"))+2, sticky = "ew")'
            )

            # Update row_adder and separator_adder variables
            row_adder += 2
            separator_adder += 2

            # Add the executed self.labels to the non_iterated_labels list
            self.non_iterated_labels.append(name_of_dataset)

        # ---------------------
        # Create column header widgets
        self.datasource_label = ttk.Label(
            self.record_frame,
            text="Datasource",
            font=f"Helvetica {self.text_size} bold",
        )
        self.datasource_label.grid(row=1, column=0, columnspan=1, padx=10, pady=3)

        # Create column header labels and place all them on row 1
        for column_title in config.options("column_headers_and_order"):
            # Remove spaces from the user input and split them into different components

            col_header = (
                config["column_headers_and_order"][column_title]
                .replace(" ", "")
                .split(",")
            )

            exec(
                f'self.{column_title} = ttk.Label(self.record_frame,text="{col_header[0]}",font=f"Helvetica {self.text_size} bold")'
            )

            exec(
                f"self.{column_title}.grid(row=1,column=col_header[1],columnspan=1,sticky=tkinter.W, padx=10, pady=3)"
            )

            # Add the executed self.labels for the column headers to the non_iterated_labels list
            self.non_iterated_labels.append(column_title)

        # ---------------------
        # Create main labels that will contain all the data

        # Step 1 - Work out which rows will contain each dataset
        row_adder = 1
        dataset_row_num = []

        for i in range(len(config.options("dataset_names"))):
            row_adder += 2
            dataset_row_num.append(row_adder)

        # Step 2 - Create a list of the dataset names and differentiate the first from the rest
        name_of_datasets = [
            config["dataset_names"][dset] for dset in config.options("dataset_names")
        ]

        # grab the dataset names that need to be highlighted if the button is clicked
        dataset_names_to_highlight = []
        # collect the dataset names entered
        for dataset_names in config.options("dataset_names"):
            dataset_names_to_highlight.append(config["dataset_names"][dataset_names])

        # remove the first dataset as this is the highlighted one.
        dataset_names_to_highlight.pop(0)

        # Create some dictionary variables to hold the highlighter and comparitor rows
        self.datarow_to_compare = {}
        self.datarows_to_highlight = {}

        # Step 3 - Create each row label and position them in the record_frame
        i = 0

        for columnfile_title in config.options("columnfile_info_and_order"):
            # Remove spaces from the user input and split them into different components
            col_header = (
                config["columnfile_info_and_order"][columnfile_title]
                .replace(" ", "")
                .split(",")
            )

            # create a text label
            exec(
                f'self.{col_header[0]} = tkinter.Text(self.record_frame,height=1,relief="flat",bg="gray93")'
            )
            # Enter in the text from the df
            exec(
                f'self.{col_header[0]}.insert("1.0",working_file["{col_header[0]}"][self.record_index])'
            )
            # configure Text so that it is a specified width, font and cant be interacted with
            exec(
                f'self.{col_header[0]}.config(width=len(working_file["{col_header[0]}"][self.record_index])+10,font=f"Helvetica {self.text_size} {self.text_bold}",state=tkinter.DISABLED)'
            )

            # cycle through each dataset name to know which row to put the label on
            for name in name_of_datasets:
                if col_header[1] == name:
                    # Colheader has matched

                    # position it on the screen
                    exec(
                        f'self.{col_header[0]}.grid(row=dataset_row_num[i],column=col_header[2],columnspan=1,padx=10, pady=3,sticky="w")'
                    )

                    # check whether it is a dataset row to highlight or not
                    if col_header[1] in dataset_names_to_highlight:
                        if col_header[2] in self.datarows_to_highlight:
                            self.datarows_to_highlight[col_header[2]].append(
                                col_header[0]
                            )

                        else:
                            self.datarows_to_highlight[col_header[2]] = [col_header[0]]

                    else:
                        self.datarow_to_compare[col_header[2]] = [col_header[0]]
                    # break the for loop if name has been resolved
                    break

                else:
                    # Colheader has not matched
                    # Increase to the next row
                    i += 1

            # Reset the iterator variable
            i = 0

            self.iterated_labels.append(col_header[0])

        if not self.show_hide_diff:
            self.show_hide_diff = 1

            self.show_hide_differences()
        else:
            self.show_hide_diff = 0

            self.show_hide_differences()

        # ---------------------
        # Sort out the buttons by frame

        # =====  record_frame

    def draw_button_frame(self):
        # Match/Non-Match buttons
        self.match_button = tkinter.Button(
            self.button_frame,
            text="Match",
            font=f"Helvetica {self.text_size}",
            command=lambda: self.update_index(1),
            bg="DarkSeaGreen1",
        )
        self.match_button.grid(row=0, column=0, columnspan=1, padx=15, pady=10)
        self.non_match_button = tkinter.Button(
            self.button_frame,
            text="Non-Match",
            font=f"Helvetica {self.text_size}",
            command=lambda: self.update_index(0),
            bg="light salmon",
        )
        self.non_match_button.grid(row=0, column=1, columnspan=1, padx=15, pady=10)

        # Add in the comment widget based on config option
        if int(config["custom_settings"]["commentbox"]):
            # create comments column if one doesn't exist
            if "Comments" not in working_file:
                working_file["Comments"] = ""

            # Get the position info from button 1
            info_button = self.match_button.grid_info()

            self.comment_label = ttk.Label(
                self.button_frame,
                text="Comment:",
                font=f"Helvetica {self.text_size} bold",
            )
            self.comment_label.grid(
                row=info_button["row"] + 1, column=0, columnspan=1, sticky="e"
            )

            self.comment_entry = ttk.Combobox(self.button_frame)
            self.comment_entry.grid(
                row=info_button["row"] + 1,
                column=1,
                columnspan=3,
                sticky="sew",
                padx=5,
                pady=5,
            )

            if (config["custom_settings"]["comment_values"]) is not None:
                self.comment_entry["values"] = (
                    config["custom_settings"]["comment_values"]
                ).split(",")

        # =====  toolFrame

    def draw_tool_frame(self):
        # Configure the grid so the record counter can move to the right
        # of the tool frame.
        self.toolFrame.grid_columnconfigure(9, weight=1)

        # Create labels for tools bar
        self.separator_tf_1 = ttk.Separator(self.toolFrame, orient="vertical")
        self.separator_tf_1.grid(
            row=0, column=3, rowspan=1, sticky="ns", padx=10, pady=5
        )
        self.separator_tf_2 = ttk.Separator(self.toolFrame, orient="vertical")
        self.separator_tf_2.grid(
            row=0, column=7, rowspan=1, sticky="ns", padx=10, pady=5
        )

        # Back button
        back_symbol = "\u23ce"
        self.back_button = tkinter.Button(
            self.button_frame,
            text=f"Back {back_symbol}",
            font=f"Helvetica {self.text_size}",
            command=lambda: self.go_back(),
        )
        self.back_button.grid(row=0, column=2, columnspan=1, padx=15, pady=10)
        # Show hide differences
        self.showhidediff = tkinter.Button(
            self.toolFrame,
            text="Show/Hide Differences",
            font=f"Helvetica {self.text_size}",
            command=lambda: self.show_hide_differences(),
        )
        self.showhidediff.grid(row=0, column=2, columnspan=1, padx=5, pady=5)
        # Change text size buttons
        increase_text_size_symbol = "\U0001f5da"
        decrease_text_size_symbol = "\U0001f5db"

        self.text_smaller_button = tkinter.Button(
            self.toolFrame,
            text=f"{decrease_text_size_symbol}-",
            font=f"Helvetica {self.text_size + 3}",
            height=1,
            width=3,
            command=lambda: self.change_text_size(0),
        )
        self.text_smaller_button.grid(row=0, column=4, sticky="e", pady=5)
        self.text_bigger_button = tkinter.Button(
            self.toolFrame,
            text=f"{increase_text_size_symbol}+",
            height=1,
            width=3,
            font=f"Helvetica {self.text_size + 3}",
            command=lambda: self.change_text_size(1),
        )
        self.text_bigger_button.grid(row=0, column=5, sticky="w", pady=5, padx=2)
        # Make text bld button
        bold_symbol = "\U0001d5d5"
        self.bold_button = tkinter.Button(
            self.toolFrame,
            text=f"{bold_symbol}",
            font=f"Helvetica {self.text_size + 3}",
            height=1,
            width=3,
            command=lambda: self.make_text_bold(),
        )
        self.bold_button.grid(row=0, column=6, sticky="w", pady=5)
        # Save and close button
        save_symbol = "\U0001f4be"
        self.save_button = tkinter.Button(
            self.toolFrame,
            text=f"Save and Close {save_symbol}",
            font=f"Helvetica {self.text_size}",
            command=lambda: self.save_and_close(),
        )
        self.save_button.grid(row=0, column=8, columnspan=1, sticky="e", padx=5, pady=5)

        # Current record pair counter.
        count_msg = f"Record pair: {self.record_index + 1} / {self.num_records}"
        self.counter_matches = ttk.Label(
            self.toolFrame, text=count_msg, font=f"Helvetica {self.text_size}"
        )
        self.counter_matches.grid(row=0, column=9, padx=10, sticky="e")

    def show_hide_differences(self):
        if not self.show_hide_diff:
            # make show show diff variable 1 so that next time this function is
            # called it will remove tags
            self.show_hide_diff = 1
            # Create a dictionary variable of the columns with differences and
            # thier label names
            self.difference_col_label_names = {}

            # for key in datarows that need to be highlighted
            for key in self.datarows_to_highlight:
                # For the values in datarows that need to be highlighted
                for vals in self.datarows_to_highlight[key]:
                    # some empty variables to control the flow of the difference indicator
                    char_consistent = []
                    container = []
                    string_start = 1
                    string_end = 0
                    count = 0

                    # For each character between the first row label and the
                    # rows underneath it
                    for char_comparison, char_highlight in zip(
                        working_file[self.datarow_to_compare[key][0]][
                            self.record_index
                        ],
                        working_file[f"{vals}"][self.record_index],
                    ):
                        # if the comparison char is not the same as the highlighter char
                        if char_comparison != char_highlight:
                            # if this is the first diff then remove them
                            if string_start:
                                # start the container values
                                container.append(count)

                                string_start = 0

                            # if we are at the end of string comparison
                            if count == min(
                                len(
                                    working_file[self.datarow_to_compare[key][0]][
                                        self.record_index
                                    ]
                                )
                                - 1,
                                len(working_file[f"{vals}"][self.record_index]) - 1,
                            ):
                                container.append(count + 1)
                                # pass this start and end values to the overall container
                                char_consistent.append(container)

                        else:
                            # if string end == string start
                            if string_end == string_start:
                                # add it to the container to complete the char number
                                # differences
                                container.append(count)

                                # restart this variable
                                string_start = 1

                                # pass this start and end values to the overall container
                                char_consistent.append(container)

                                container = []
                        # increase the count
                        count += 1

                    # if length of the comparator is less highlighter make it yellow as well
                    if len(
                        working_file[self.datarow_to_compare[key][0]][self.record_index]
                    ) < len(working_file[f"{vals}"][self.record_index]):
                        char_consistent.append(
                            [
                                len(
                                    working_file[self.datarow_to_compare[key][0]][
                                        self.record_index
                                    ]
                                ),
                                len(working_file[f"{vals}"][self.record_index]),
                            ]
                        )

                        count = 0

                    # for each tag number in char consistent create the tag and save the tag name information
                    for tag_adder in range(len(char_consistent)):
                        if vals in self.difference_col_label_names:
                            self.difference_col_label_names[vals].append(
                                f"{vals}_diff{str(tag_adder)}"
                            )

                        else:
                            self.difference_col_label_names[vals] = [
                                f"{vals}_diff{str(tag_adder)}"
                            ]

                        exec(
                            f'self.{vals}.tag_add(f"{vals}_diff{str(tag_adder)}",f"1.{char_consistent[tag_adder][0]}", f"1.{char_consistent[tag_adder][-1]}")'
                        )

                        exec(
                            f'self.{vals}.tag_config(f"{vals}_diff{str(tag_adder)}",background="yellow",foreground = "black")'
                        )

        else:
            # reset this variable
            self.show_hide_diff = 0
            # for all variable labels with differences - remove the tag labels
            for key in self.difference_col_label_names:
                for vals in self.difference_col_label_names[key]:
                    exec(f"self.{key}.tag_remove('{vals}','1.0','end')")

    def make_text_bold(self):
        """
        Makes the text bold or not
        Returns
        -------
        None.
        """
        if not self.text_bold_boolean:
            self.text_bold_boolean = 1
            self.text_bold = "bold"

        else:
            self.text_bold_boolean = 0
            self.text_bold = ""

        self.update_gui()

    def get_starting_index(self):
        """
        This function finds the first row in column index [-1] that has a value not equal to zero or 1
        and returns the index number

        Parameters
        ----------
        None

        Returns
        -------
        i : int
            index number that relates to the next record to be matched

        """
        # get a list of index_values
        index_values = list(range(0, len(working_file)))

        # cycle through the working_file dataset to determine the next avaliable record

        # Choose which one to cycle through
        if int(config["custom_settings"]["commentbox"]):
            for i in index_values:
                if working_file.iloc[i, -2] != 1 and working_file.iloc[i, -2] != 0:
                    return i
                elif i == self.num_records - 1:
                    return i
                elif working_file.iloc[i, -2] == 1 or working_file.iloc[i, -2] == 0:
                    pass

        else:
            for i in index_values:
                if working_file.iloc[i, -1] != 1 and working_file.iloc[i, -1] != 0:
                    return i
                elif i == self.num_records - 1:
                    return i
                elif working_file.iloc[i, -1] == 1 or working_file.iloc[i, -1] == 0:
                    pass

    def update_gui(self):
        """
        A simple function that updates the different GUI labels based on the
        records.

        Parameters
        ----------
        None

        Returns
        -------
        None.
        """
        if self.check_matching_done() == 0:
            # configure the non-iterable labels
            for non_iter_columns in self.non_iterated_labels:
                exec(
                    f'self.{non_iter_columns}.config(font=f"Helvetica {self.text_size} bold")'
                )

            for widget in self.record_frame.winfo_children():
                widget.destroy()

            for widget in self.toolFrame.winfo_children():
                widget.destroy()

            for widget in self.button_frame.winfo_children():
                widget.destroy()
            self.draw_record_frame()
            self.draw_button_frame()
            self.draw_tool_frame()
            if not self.show_hide_diff:
                self.show_hide_diff = 1

                self.show_hide_differences()
            else:
                self.show_hide_diff = 0

                self.show_hide_differences()
            if self.record_index == 0:
                self.back_button.config(state=tkinter.DISABLED)
            else:
                self.back_button.config(state="normal")
        elif self.check_matching_done() == 1:
            tkinter.messagebox.showinfo(
                "Matching Finished",
                "Maching Finished Press save and close or use the back button to return to the previous record",
            )

    def update_df(self, match_res):
        """
        Updates the dataframe with the matching outcome. 1 == Match, 0 == Non-match
        Parameters
        ----------
        match_res : int - boolean.
            Adds a 1 or a 0 in the column
        Returns
        -------
        None.
        """

        # update df
        working_file.at[self.record_index, "Match"] = match_res

        if int(config["custom_settings"]["commentbox"]):
            working_file.at[self.record_index, "Comments"] = self.comment_entry.get()

    def save_at_checkpoint(self):
        """
        This function saves the data every at an interval defined in the congig file (num_records_checkpoint).
        This back up the data to that point.
        Returns
        -------
        None.
        """

        # check whether the record_index is a multiple of recods_per_checkpoint & record_index is less than total num records
        if (self.record_index % self.records_per_checkpoint == 0) & (
            self.record_index < self.num_records
        ):
            # checkpoint it by saving it
            working_file.to_csv(self.filename_old, index=False)
            # increase checkpoint counter
            self.checkpointcounter += 1

        # Check if record_index is a multiple of 5 & record_index== num records
        elif (self.record_index % self.records_per_checkpoint == 0) & (
            self.record_index == self.num_records
        ):
            # Save it as DONE
            os.rename(self.filename_old, self.filename_done)
            working_file.to_csv(self.filename_done, index=False)
            self.checkpointcounter += 1

        elif self.record_index % self.records_per_checkpoint != 0:
            pass

    def check_matching_done(self):
        """
        This function checks if the number of iterations is greater than the number of
        rows; and breaks the loop if so.
        Returns
        -------
        Boolean value, this dictates whether to stop displaying any more records
        and close the app or continue updating the app
        1 = Stop The GUI
        0 = Continue updating the GUI
        """
        # Query whether the current record matches the total number of records (end of the terminal)
        if self.record_index > (self.num_records - 1):
            # disable the match and Non-match buttons
            self.match_button.configure(state=tkinter.DISABLED)
            self.non_match_button.configure(state=tkinter.DISABLED)
            # present a message on the screen infomring the user that matching is finished
            self.matchdone = ttk.Label(
                root, text="Matching Finished. Press save and close.", foreground="red"
            )
            self.matchdone.grid(row=1, column=0, columnspan=1)

            return 1
        else:
            return 0

    def save_and_close(self):
        """
        This function saves the working_file dataframe and closes the GUI
        Parameters
        ----------
        filepath : string type
            This should be the exact directory that will be saved.
        Returns
        -------
        None.
        """
        # Check whether matching has now finished (i.e. they have completed all records)
        if self.record_index == (self.num_records):
            # if matching is now complete rename the file
            if self.num_records % self.records_per_checkpoint != 0:
                os.rename(self.filename_old, self.filename_done)
                working_file.to_csv(self.filename_done, index=False)
            elif self.num_records % self.records_per_checkpoint == 0:
                working_file.to_csv(self.filename_done, index=False)

        else:
            # If not it yet finshed save it using the old file name
            working_file.to_csv(self.filename_old, index=False)

        # close down the app
        root.destroy()

    def update_index(self, event):
        """
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
        """
        # Update the Match Column with the Matchers Choice
        self.update_df(event)

        # Update the record_index
        self.record_index += 1

        # check if at checkpoint
        self.save_at_checkpoint()

        stp_gui = self.check_matching_done()

        # Check if reached the end of the script
        if stp_gui:
            pass
            # could add in additional functionality here to do with saving the working_file file
        else:
            # Update the GUI labels
            self.update_gui()

    def go_back(self):
        """
        This function allows users to go back through records and change any of their matching decisions

        Parameters
        ----------
        None
        Returns
        -------
        None.

        """
        # If they have reached the end of matching
        if self.record_index == len(working_file):
            # Take away the Matching is finished message
            self.matchdone.grid_forget()
            # Reactivate the buttons
            self.match_button.configure(state="normal")
            self.non_match_button.configure(state="normal")
            # Update the record_index
            self.record_index = self.record_index - 1
            # update the overall gui
            self.update_gui()
            # rename the file back to in progress
            if len(working_file) % self.records_per_checkpoint == 0:
                os.rename(self.filename_done, self.filename_old)
        elif self.record_index > 0:  # If they are part way through matching
            # update the record_index
            self.record_index = self.record_index - 1
            # update the gui
            self.update_gui()
        elif self.record_index == 0:
            pass

    def change_text_size(self, size_change):
        """
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
        """
        # depending on the argument passed - increase or decrease the text size/geometry paramaters
        if size_change:
            self.text_size += 1

        else:
            self.text_size -= 1
            # # if commentbox specified in config

        # clear recordframe
        self.update_gui()

    def on_exit(self):
        """
        When you click to exit, this function is called, which creates a message
        box that questions whether the user wants to Exit without saving

        """
        # if they click yes
        if tkinter.messagebox.askyesno(
            "Exit", "Are you sure you want to exit WITHOUT saving?"
        ):
            # check if this is the first time they are accessing it
            if not self.matching_previously_began & self.checkpointcounter != 0:
                # then rename the file removing their intial and 'inProgress' tag
                os.rename(
                    self.filename_old,
                    "_".join(self.filename_old.split("_")[0:-2]) + ".csv",
                )

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
    config.read("Config_pairwise.ini")

    # Get the initial directory folder
    initdir = config["matching_files_details"]["file_pathway"]

    # specify file types - this will only show these files when the dialog box opens up
    filetypes = (("csv files", "*.csv"),)

    # grab user credentials
    user = getpass.getuser()

    # ===================== Open Intro GUI
    # Open a file pen dialog box, allow user to choose file, then grab user credentials
    root = tkinter.Tk()
    # Run the Intro GUI
    intro = IntroWindow(root, initdir, filetypes)

    root.mainloop()

    # END OF STEP 1

    # ------
    # Step 2:
    # ------

    # ------- Create filepath variables, load in the selected data and specify column variables

    # Check if the user running it has selected this file before (this means they have done some of the matching already and are coming back to it)
    if "inProgress" in intro.fileselect.split("/")[-1]:
        # If it is the same user
        if user in intro.fileselect.split("/")[-1]:
            # Dont rename the file
            renamed_file = intro.fileselect

            # create the filepath name for when the file is finished
            filepath_done = f"{'/'.join(renamed_file.split('/')[:-1])}/{renamed_file.split('/')[-1][0:-15]}_DONE.{renamed_file.split('/')[-1].split('.')[-1]}"

        else:
            # Rename the file to contain the additional user
            renamed_file = f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1].split('.')[0][0:-11]}_{user}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"
            os.rename(rf"{intro.fileselect}", rf"{renamed_file}")

            # create the filepath name for when the file is finished
            filepath_done = f"{'/'.join(renamed_file.split('/')[:-1])}/{renamed_file.split('/')[-1][0:-15]}_DONE.{renamed_file.split('/')[-1].split('.')[-1]}"

    # If a user is picking this file again and its done
    elif "DONE" in intro.fileselect.split("/")[-1]:
        # If it is the same user
        if user in intro.fileselect.split("/")[-1]:
            # dont change filepath done - keep it as it is
            filepath_done = intro.fileselect

            # Rename the file
            renamed_file = f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1][0:-9]}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"
            os.rename(rf"{intro.fileselect}", rf"{renamed_file}")
        else:
            # If it is a different user
            # Rename the file to include the additional user
            renamed_file = f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1].split('.')[0][0:-5]}_{user}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"
            os.rename(rf"{intro.fileselect}", rf"{renamed_file}")

            # create the filepath done
            filepath_done = f"{'/'.join(renamed_file.split('/')[:-1])}/{renamed_file.split('/')[-1][0:-15]}_DONE.{renamed_file.split('/')[-1].split('.')[-1]}"

    else:
        # Resave this file with the user ID at the end so no one else selects it
        # rename it with '_inProgress' and their entered initials
        renamed_file = f"{'/'.join(intro.fileselect.split('/')[:-1])}/{intro.fileselect.split('/')[-1].split('.')[0]}_{user}_inProgress.{intro.fileselect.split('/')[-1].split('.')[-1]}"
        os.rename(rf"{intro.fileselect}", rf"{renamed_file}")

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
    root = tkinter.Tk()
    mainWindow = ClericalApp(root, working_file, filepath_done, renamed_file, config)
    root.mainloop()

    print(
        "\n Number of records matched:",
        str(len(working_file[working_file.Match != ""])),
    )
