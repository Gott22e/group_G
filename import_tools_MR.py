# Written by Mae Rose Gott
# (Small modifications from Allison Nau)

# Run this first
# imports
import pandas as pd


class allisort():
    def __init__(self, fileIn, keys, values, file_out=False, merge={}, nan=5, col=False, add=False):
        '''
        fileIn: (required) file to open. MUST be csv
        keys: ["ColumnName_1", "ColumnName_2, ... , "ColumnName_N"] List of strings. Will be column headers
            ColumnName_1 will hold the names of the expanded values (Names of Columns specified in values)
            ColumnName)_N is the last column, will hold the values under the Corresponding columnName_1
            ColumnName_2 to ColumnName_(N-1),please specify these if you specified anything in "add"
        values: [int, int, ... , int] List of ints; Columns in the original Data Frame to expand
        col: (optional) int. If column names are on a different line, specify the line
        #????? merge: (optional) [] array of files to merge with file_name. Useful if there are multiple files to merge
        nan: (Optional) int. Minimum number of filled cells before row is deleted
        file_out: (optional) file to write out to. If none are specified then will just re-write fileIn
        add: (optional) {"letter(s)":[column int(s)], "":[],..."":[]} Dictionary of letters to arrays.
        Defines if specific modifiers need to be added to a column, ie units
        '''

        self.f = pd.read_csv(fileIn)
        self.f = self.f.loc[:, ~self.f.columns.str.contains('^Unnamed')]  # deletes unnamed/blank columns
        self.splitOn = "|"
        self.fileIn = fileIn
        # Rename columns if needed
        if type(col) == int:
            self.f.columns = self.f.iloc[col,]
            for i in range(col + 1):
                self.f = self.f.drop([i, ])
        # TODO: handle for multiple sheets
        # Define file name to output to
        self.outFile = self.setOutputFilename(file_out)

        # Changes column names if a dict was entered for add
        self.add_to_col(add)

        self.DF = self.expandDF(keys, values, self.f)

        # merges DF
        self.DF = self.mergeDF(merge)
        self.DF.to_csv(self.outFile, index=False)

    """Adds the key of a dict to the int values defined for it """

    def add_to_col(self, add):
        l = list(self.f.columns)
        if type(add) == dict:
            for key, value in add.items():
                for i in value:
                    if i in range(len(l)):
                        l[i] += "|%s" % (str(key))
                    else:
                        print(l)
                        print(
                            "{0} in key {1} is not a recognized value! What would you like to do? Skip it = S; Exit = E".format(
                                i, key))
                        a = input().upper()
                        while a != "E" and a != "S":
                            print(
                                "{0} is not a valid command!\nTo exit and try again, enter \"E\"\nTo skip it, enter \"S\"".format(
                                    a))
                            a = input().upper()
                        if a == "E":
                            return False

        elif type(add) == str:
            self.sepOn = add

        self.f.columns = l

    """Input a dict of {table_name:column_to_merge_on}"""

    def mergeDF(self, dic):
        for key, value in dic.items():
            new = pd.read_csv(key)
            print(new.head(5))
            self.DF = self.DF.merge(new, how='left', on=value)
        return self.DF

    """Input a string, outputs a file name for a csv file"""

    def setOutputFilename(self, file_out):
        # Define file name to output to
        outFile = ""
        if type(
                file_out) == bool:  # if False use the fileIn name, but save it to the current directory (deletes everything before the last "/")
            outFile = self.fileIn
            if "/" in outFile:  # Ensures it is saved to current directory, unless "/" is used then good luck
                outFile = outFile.split("/")
                outFile = outFile[len(outFile) - 1]
                outFile = ''.join(outFile)
            outFile = "output_" + outFile
        else:
            outFile = file_out
        # Ensures that file ends with a .csv. Deletes anyting after the first "."
        outFile = outFile.split(".")
        if len(outFile) > 1:
            outFile = outFile[:len(outFile) - 1]
        outFile = ''.join(outFile)
        outFile += ".csv"
        return outFile

    """Actually expands the data frame based on the key and values"""

    def expandDF(self, key, values, f):
        df = f.copy()
        c = list(df.columns)
        # Change from intarray to stringarray. Allows for modifications
        names = []  # analyte names
        for i in values:
            if type(i) == int:
                try:
                    names.append(c[i])
                except:
                    print(i)
            elif i in c:  # allows for functionality with actually specifying names
                names.append(i)
                # Keeps only the columns not specified for expansion
        for n in names:
            if n in c:
                c.remove(n)
        # Creat a temp for the output
        temp = pd.DataFrame(columns=c)
        # Add columns according to the column names specified in Key
        for k in key:
            temp[k] = None

        # Expands each row by analyte in Names
        for i in range(len(df.iloc[:, 0])):
            y = {}  # Holds the values that are kept the same when expanding the row
            # Adds those values here
            for x in c:
                y[x] = (df.iloc[i][x])
            # Fore each we expand upon
            for analyte in names:
                other = y.copy()  # New dict that is copy of y
                a = analyte.split(self.splitOn)  # Split analyte by |, incase we used the add argument

                # This part adds columns if there weren't enough
                st = 0
                while len(key) < len(a) + 1:
                    key.append("Allisort_Val_" + str(st))
                    st += 1

                # adds the values to the appropriate place in the dictionary
                for k in range(len(key)):
                    if k < len(a):
                        other[key[k]] = a[k]
                    else:
                        other[key[k]] = df.iloc[i][analyte]

                temp = temp.append(other, ignore_index=True)  # adds to temp
        return temp