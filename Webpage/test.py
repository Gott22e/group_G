#!/usr/bin/python
import pymysql
import sys
import cgi

import cgitb
cgitb.enable()

# print content-type
print("Content-type: text/html\n")

print("<html><head>")
print("<title>Upper Columbia River Site Database</title>") #Figure out the #miRNA
print('''<style>
body {margin:30;padding:30}
#miRNA {
  font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
  border-collapse: collapse;
  width: 50%;
}

#miRNA td, #miRNA th {
  border: 1px solid #ddd;
  padding: 8px;
}

#miRNA tr:nth-child(even){background-color: #f2f2f2;}

#miRNA tr:hover {background-color: #ddd;}

#miRNA th {
  padding-top: 12px;
  padding-bottom: 12px;
  text-align: left;
  background-color: #4CAF50;
  color: white;
}
</style>
</head>''')
print("</head>")
print("<body>")

print("<h1>Search Database</h1>")
print('''<form name="Columbia River DB" onsubmit="return checkInput();" action="https://bioed.bu.edu/cgi-bin/students_21/group_proj/group_G/test.py" method="POST" >
        Analyte: <input type="text" id = "name" name="name" value =""/>
#create queries
query = """SELECT analyte,upper_depth,lower_depth FROM cr_2"""
connection = pymysql.connect(user="test", password="test", db="group_G", port=4253)

#create cursor
cursor = connection.cursor()
cursor.execute(query)
results = cursor.fetchall()
analyte_info = []
for info in results:
    analyte_info += info

cursor.close()
connection.close()

#get forms
form = cgi.FieldStorage()
analyte = form.getvalue("name")
upper_depth = form.getvalue("upper depth")
lower_depth = form.getvalue("lower depth")

if analyte:
        print("<h2>Results</h2>")
        print("<table id=Analyte Results>")
        print("<tr><th>Analyte</th><th>Upper Depth</th><th>Lower Depth</th></tr>")

        if upper_depth:
                select = """SELECT analyte, upper_depth,lower_depth FROM cr_2 WHERE analyte = '%s' AND upper_depth = %s AND lower_depth = %s;""" %(analyte,upper_depth,lower_depth)
        else:
                select = """SELECT analyte, upper_depth, lower_depth FROM cr_2 WHERE analyte = '%s';""" %(analyte)

        connection = pymysql.connect(user="test",password="test",db="group_G",port=4253)
                cursor.execute(select)
                for row in cursor.fetchall():
                        print("<tr><td>%s</td><td>%f</td><td>%f</td></tr>" %(row[0],row[1],row[2]))
        cursor.close()
        connection.close()

        print("</table>")

print('''
<script type="text/javascript">
function checkInput() {
    var analyte = document.getElementById('name').value;
    var upper_depth = document.getElementById('upper depth').value;
    var lower_depth = document.getElementById('lower depth').value;

    // check out whether analyte is empty.
    if (analyte == "") {
        alert("You did not enter an analyte!");
        return false;
    }
    //
    else if (analyte.indexOf(analyte) == -1 ) {
        alert("Analyte not found");
        return false;
    }
    else {
        return true;
  }
}
</script>
''') % analyte_info

print("</body></html>")
