import DSSClient
import getopt
import sys
from getpass import getpass

dss_username = None
dss_password = None
schedule_name = None
file_type = "all"
aws_download = False
dss_client  = DSSClient.DSSClient()

def check_arguments():
    global dss_password, dss_username
    file_types = ['all','note','ric','data']
    if dss_username == None:
        print("dss_username is required")
        print_usage()
        sys.exit(2)
    
    if dss_password == None:
        dss_password = getpass('DSS Password:')   

    if file_type not in file_types:
        print("file_type must be all, note, ric, or data")
        print_usage()
        sys.exit(2)

def show_all_schedule(schedules):
    if schedules != None:
        if "value" in schedules:
            for schedule in schedules["value"]:
                if schedule["Trigger"]["@odata.type"] != "#DataScope.Select.Api.Extractions.Schedules.ImmediateTrigger":
                    print("-\t ",schedule["Name"], ":", schedule["Trigger"]["@odata.type"])

def print_usage():
    print('Usage: DownloadScheduledExtractedFiles_python.py [-u dss_username] [-p dss_password] [-s schedule_name] [-f file type (all, note, ric, data) ] [-x] [--help]')

if __name__ == "__main__":


    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:p:s:f:x", ["help"])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("--help"):
            print_usage()
            sys.exit(0)
        elif opt in ("-u"):
            dss_username = arg
        elif opt in ("-p"):
            dss_password = arg
        elif opt in ("-s"):
            schedule_name = arg
        elif opt in ("-f"):
            file_type = arg
        elif opt in ("-x"):
            aws_download = True
    
    check_arguments()
    
    

    try:
        dss_client.login(dss_username,dss_password)
        print("Login succeeded")
        if schedule_name == None:
            schedules = dss_client.list_all_schedules()
            show_all_schedule(schedules)
        else:
            schedule = dss_client.get_schedule_by_name(schedule_name)
            print("\nThe schedule ID of", schedule["Name"], "is", schedule["ScheduleId"], "(",schedule["Trigger"]["@odata.type"],")");
            last_report_extraction = dss_client.get_last_extraction(schedule)
            print("\nThe last extraction was extracted on", last_report_extraction["ExtractionDateUtc"],"GMT");
            
            if file_type == "all":
                extracted_files = dss_client.get_all_files(last_report_extraction)
                if "value" in extracted_files:
                    for file in extracted_files["value"]:                      
                        print("\n"+file["ExtractedFileName"], "(", file["Size"],"bytes) is available on the server.")
                        print("\nDownloading...")
                        dss_client.download_file(file, aws_download)
                        print("\nDownload Completed")
            else:
                extracted_file = dss_client.get_file(last_report_extraction, file_type)
                print("\n"+extracted_file["ExtractedFileName"], "(", extracted_file["Size"],"bytes) is available on the server.")
                print("\nDownloading...")
                dss_client.download_file(extracted_file, aws_download)
                print("\nDownload Completed")

    except Exception as ex:     
        print("Exception", ex);