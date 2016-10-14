package org.microsoft;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Calendar.*;

public class BlobDownload {

    enum logheader {version_number,request_start_time,operation_type,request_status,http_status_code,end_to_end_latency_in_ms,server_latency_in_ms,authentication_type,requester_account_name,owner_account_name,service_type,request_url,requested_object_key,request_id_header,operation_count,requester_ip_address,request_version_header,request_header_size,request_packet_size,response_header_size,response_packet_size,request_content_length,request_md5,server_md5,etag_identifier,last_modified_time,conditions_used,user_agent_header,referrer_header,client_request_id};
    static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    static final String LOGS = "/$logs/";
    static final String TYPE = "blob/";
    static final char DELIMITER = ';';

    public static void main(String[] args) {
        if(args.length<4){
            System.err.println("Usage: azlogs <AccountName> <AccountKey> startDate(seconds since epoch) endDate(seconds since epoch) [columns(sorted)]");
            System.exit(1);
        }
        boolean filter = false;
        try {
            String storageConnectionString = "DefaultEndpointsProtocol=http;" + "AccountName="+args[0] + ";AccountKey="+args[1];
            CloudBlobContainer container = CloudStorageAccount.parse(storageConnectionString).createCloudBlobClient().getContainerReference(LOGS);
            Calendar start = getInstance(); start.setTimeInMillis(Long.parseLong(args[2])*1000);
            Calendar end = getInstance(); end.setTimeInMillis(Long.parseLong(args[3])*1000);
            ArrayList<Integer> indexes = new ArrayList<Integer>();
            if(args.length>4 && !args[4].isEmpty()){
                filter=true;
                String header = "";
                for (String s : args[4].split(",")) {
                    s= s.trim();
                    header += DELIMITER+s;
                    indexes.add(logheader.valueOf(s).ordinal());
                }
                System.out.println(header.replaceFirst(String.valueOf(DELIMITER),""));
            }else {
                System.out.println(Arrays.toString(logheader.values()).replace(",",";").replace("[","").replace("]","").replace(" ",""));
            }

            System.err.println("Collecting logs from : " + start.getTime() + " to : " + end.getTime());

            for (ListBlobItem yearFolder: container.listBlobs("blob/")) {
                int year = getNumberFromPath(yearFolder,LOGS + TYPE);
                if(year >= start.get(YEAR) && year <= end.get(YEAR)) {
                    String yearString = String.format(TYPE+"%04d/",year);
                    for (ListBlobItem monthFolder : container.listBlobs(yearString)) {
                        int month = getNumberFromPath(monthFolder,LOGS+yearString);
                        Calendar monthStart = Calendar.getInstance(); monthStart.set(year,month-1,1); monthStart.set(year,month-1,monthStart.getActualMinimum(DAY_OF_MONTH),0,0,0);
                        Calendar monthEnd = Calendar.getInstance(); monthEnd.set(year,month-1,1); monthEnd.set(year,month-1,monthEnd.getActualMaximum(DAY_OF_MONTH),23,59,59);
//                        System.out.println("MONTH : "+ monthStart.getTime() + " : " + start.getTime() +" : " +monthStart.after(start) + ": " + monthEnd.getTime() + " : " + end.getTime() + " : " +monthEnd.before(end));
                        if((start.after(monthStart) && start.before(monthEnd)) || (end.after(monthStart) && end.before(monthEnd))) {
                            String monthString = String.format("%s%02d/",yearString,month);
                            for (ListBlobItem dayFolder : container.listBlobs(monthString)) {
                                int day = getNumberFromPath(dayFolder,LOGS+monthString);
                                Calendar dayStart = Calendar.getInstance(); dayStart.set(year,month-1,1); dayStart.set(year,month-1,day,0,0,0);
                                Calendar dayEnd = Calendar.getInstance(); dayEnd.set(year,month-1,1); dayEnd.set(year,month-1,day,23,59,59);
//                                System.out.println("DAY : " + dayStart.getTime() + " : " + start.getTime() +" : " +dayStart.before(start) + ": " +
//                                        dayEnd.getTime() + " : " + end.getTime() + " : " +dayEnd.before(end));
                                if((start.after(dayStart) && start.before(dayEnd)) || (end.after(dayStart) && end.before(dayEnd))) {
                                    String dayString = String.format("%s%02d/",monthString,day);
                                    for (ListBlobItem hourFolder : container.listBlobs(dayString)) {
                                        int hh = Integer.parseInt(hourFolder.getUri().getPath().replace(LOGS+dayString,"").replace("/","").substring(0,2));
                                        if(hh >= start.get(HOUR_OF_DAY) && hh <= end.get(HOUR_OF_DAY)) {
                                            for (ListBlobItem logFile : container.listBlobs(hourFolder.getUri().getPath().replace(LOGS, ""))) {
                                                System.err.println("Downloading : " + logFile.getUri());
                                                for (String line : container.getBlockBlobReference(logFile.getUri().getPath().replace(LOGS, "")).downloadText().split("\n")) {
                                                    Date request_time = dateFormat.parse(filterColumns(line, Collections.singletonList(logheader.request_start_time.ordinal())));
                                                    if (request_time.after(start.getTime()) && request_time.before(end.getTime())) {
                                                        System.out.println(filter? filterColumns(line, indexes) : line);
                                                    }
                                                }
                                            }
                                        }else{
                                            System.err.println("Skipping hour Folder : "+ hourFolder.getUri());
                                        }
                                    }
                                }else {
                                    System.err.println("Skipping day Folder : "+ dayFolder.getUri());
                                }
                            }
                        }else{
                            System.err.println("Skipping month Folder : "+ monthFolder.getUri());
                        }
                    }
                }else{
                    System.err.println("Skipping year Folder : "+ yearFolder.getUri());
                }
            }
        } catch (StorageException storageException) {
            System.err.print("StorageException encountered: ");
            System.err.println(storageException.getMessage());
            System.exit(-1);
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        }
    }

    static int getNumberFromPath(ListBlobItem path, String prefix){
        return Integer.parseInt(path.getUri().getPath().replace(prefix, "").replace("/", ""));
    }

    static String filterColumns(String row, List<Integer> indexes){
        String result = null;
        String[] column = row.split(String.valueOf(DELIMITER));
        for (Integer i : indexes) {
            if(result == null){
                result = column[i] ;
            } else {
                result += DELIMITER + column[i] ;
            }
        }
        return result;
    }

}