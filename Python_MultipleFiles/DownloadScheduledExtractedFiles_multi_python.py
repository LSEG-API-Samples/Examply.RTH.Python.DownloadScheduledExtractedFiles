
import DSSClient1
import getopt
import sys
from getpass import getpass
from multiprocessing import Pool, cpu_count

dss_username = None
dss_password = None
schedule_name = None
num_file = 1
dss_client  = DSSClient1.DSSClient1()

def check_arguments():
    global dss_password, dss_username

    if dss_username == None:
        print("dss_username is required")
        print_usage()
        sys.exit(2)
    
    if dss_password == None:
        dss_password = getpass('DSS Password:')   

 

def show_all_schedule(schedules):
    if schedules != None:
        if "value" in schedules:
            for schedule in schedules["value"]:
                if schedule["Trigger"]["@odata.type"] != "#DataScope.Select.Api.Extractions.Schedules.ImmediateTrigger":
                    print("-\t ",schedule["Name"], ":", schedule["Trigger"]["@odata.type"])

def print_usage():
    print('Usage: DownloadScheduledExtractedFiles_python.py [-u dss_username] [-p dss_password] [-s schedule_name] [-n number of files (1)] [--help]')

def init_pool(the_int):
    global dss_client
    dss_client = the_int

def download_file(extraction):
    #global dss_client
    #print("Download  File" + extraction)
    extracted_file = dss_client.get_file(extraction, "data")
    #print("\n"+extracted_file["ExtractedFileName"], "(", extracted_file["Size"],"bytes) is available on the server.")
    #print("\nDownloading...")
    dss_client.download_file(extracted_file, False)
    #print("\nDownload Completed")

if __name__ == "__main__":


    try:
        opts, args = getopt.getopt(sys.argv[1:], "u:p:s:n:", ["help"])
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
        elif opt in ("-n"):
            num_file = int(arg)
    
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
            all_report_extractions = dss_client.get_all_extractions(schedule)
            print(all_report_extractions)
            print(len(all_report_extractions["value"]))
            if(num_file > len(all_report_extractions["value"])):
               num_file = len(all_report_extractions["value"])

            print("There are {} CPUs on this machine ".format(cpu_count()))
            #download_file(all_report_extractions["value"][0])
            #pool = Pool(cpu_count())
            pool = Pool(initializer=init_pool, initargs=(dss_client,))
            results = pool.map(download_file, all_report_extractions["value"][:num_file])
            pool.close()
            pool.join()
            

    except Exception as ex:     
        print("Exception", ex);

            
