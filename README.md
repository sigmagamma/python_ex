# python_ex
## general
This project includes the following python modules:
### sendmessage.py
Module that sends a message to a queue of type RABBITMQ. The message will contain a full path to an SQLITE database, a name of a country and a 4-digit year.  
The module recieves input as arguments in the following syntax:  
Python sendmessage.py (server) (path) (country) (year)  
input should be provided without () signs.  
(server) - the rabbitmq server  
(path) - the path to the database used  
(country) - a name of a country  
(year) - a year  
Example:  
python sendmessage.py localhost d:\sqlite\db\chinook.db USA 2009  
The program will exit on an insufficient number of parameters or a non-numeric year parameter.
### parsemessage.py
Module that listens to a queue and when a message is received opens the database (sent in the message)  
and performas the following actions:    
1. Create CSV file containing a list of name of a country and the number of purchases performed in it  
2. Creates CSV file containing a list of name of a country and the number of items purchased in it  
3. Create a JSON file containing a name of a country and the list of all albums purchased in it  
4. Create an XML file contiaining a name of a disc, the country, amount of sales and year. the disc should be of type  
rock and the most sold in a certain state from year YYYY.  
5. Creates three tables based on the data from 1, 2 and 4. 

The module recieves input as arguments in the following syntax:  
Python parsemessage.py (server)  
input should be provided without () signs.  
(server) - the rabbitmq server  
Example:  
python parsemessage.py localhost  
The program will exit on an insufficient number of parameters.  

Note that the module deletes any exisitng messages in the queue prior to running.  
If the tables or files exist they are deleted and re-written for each message received.

### Implementation details
A. Code was written in python 3.7.0.

B. As point 4 was the only one that specifically mentioned filtering by country and year, I took the other points as an instruction to generate a list of countries without those filters. However modifying the other points for such a requirement is relatively simple.

C. Point 3 is unclear regarding "albums purchased", as the tracks are purchased, and it is unclear whether a partial purchase of tracks qualifies as a purchase of an album. Checking to see if an entire album was purchased was not implemented at this point.  

D. Similarly, point 4 refers to a disc, however as tracks are sold rather than albums, and as the type is related to a track rather than to an album, I took "disc" to mean "track" in that context.

E. The exercise was not clear on how to fill the tables with the relevant data, however reading data back from the files makes sense in the context of the exercise. Due to lack of time, I opted to write the results using the database using a manipulation of the existing queries into CREATE statements. The chosen implementation also does not create pre-defined schemas with keys, again due to lack of time, however that would be a rather simple addition.

Reading back from the files into the tables would take some more coding. It would naturally be performed in the reverse process from writing - reading the data into data structures using the relevant packages, and then parsing them into input for SQL statements that perform insertion.

### Packages used and documentation
sqlite  
https://docs.python.org/3/library/sqlite3.html  
json library  
https://docs.python.org/3/library/json.html  
Pika - rabbitmq interface tutorial  
https://www.rabbitmq.com/tutorials/tutorial-one-python.html  
ElementTree - XML API  
https://docs.python.org/3/library/xml.etree.elementtree.html
https://stackabuse.com/reading-and-writing-xml-files-in-python/

sqlite date and time functions  
https://www.sqlite.org/lang_datefunc.html  
sqlite cast expressions  
https://www.sqlite.org/lang_expr.html#castexpr  
getting dictionary from sqlite query  
https://stackoverflow.com/questions/3300464/how-can-i-get-dict-from-sqlite-query  
json examples  
https://json.org/example.html  
encoding and json  
https://stackoverflow.com/questions/18337407/saving-utf-8-texts-in-json-dumps-as-utf8-not-as-u-escape-sequence

