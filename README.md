# azlogs

Download and analyze Azure storage logs over a specific range.

# How to compile

``mvn  package assembly:single`` will create a jar with all dependency in target folder.

# How to run

``Usage: azlogs <AccountName> <AccountKey> startDate(yyyy-MM-dd HH:mm:ss) endDate(yyyy-MM-dd HH:mm:ss) [columns(sorted)]``

Here is an example

``java -jar azlogs.jar storage1 67t2Mw== "2016-09-30 00:33:00" "2016-09-30 00:59:00" "request_start_time,operation_type,end_to_end_latency_in_ms"  2>debug_logs > output``

# How to analyze

The above command generates a file that is delimited by ";". The column names are optional. Full list of columnnames are [here](https://msdn.microsoft.com/en-us/library/azure/hh343259.aspx). If you do not specify it will output all the columns. The [csvkit](https://csvkit.readthedocs.io) is great tool to run post analysis on the output. Here is an example command that generates latency summary for different operation types. 

``csvsql -d ";"  --query "select operation_type, count(*), avg(end_to_end_latency_in_ms), min(end_to_end_latency_in_ms), max(end_to_end_latency_in_ms), avg(server_latency_in_ms), min(server_latency_in_ms),max(server_latency_in_ms) from output group by operation_type"``
