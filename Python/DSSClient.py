import requests

class DSSClient(object):
    """Datascope Client"""
    __token__ = ""
    __dss_uri__ = "https://selectapi.datascope.refinitiv.com/RestApi/v1/"
    __auth_endpoint__ =  "Authentication/RequestToken"
    __schedule_by_name_endpoint__ = "Extractions/ScheduleGetByName(ScheduleName='{}')"
    __schedule_endpoint__ = "Extractions/Schedules"
    __last_extraction_endpoint__ = "Extractions/Schedules('{}')/LastExtraction"
    __all_files_endpoint__ = "Extractions/ReportExtractions('{}')/Files"
    __file_endpoint__ = "Extractions/ReportExtractions('{}')/"
    __download_file_endpoint__ = "Extractions/ExtractedFiles('{0}')/$value"
    __timeout__ = 30
    __response__ = None

    def __init__(self):
        self.__token__ = ""

    def __get__(self, url, headers, params):
        
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=self.__timeout__
                )
            
        except requests.exceptions.ReadTimeout as e:
           return None, str(e) 
        except requests.exceptions.RequestException as e:
            return None, str(e) 
        
        if(response.status_code != requests.codes.ok):
            return None, str(response.status_code)+response.content.decode("utf-8") 
        
        
        
        return response.json(), None

    def __post__(self, url, headers, body=None, filename=None):              
        try:            
            response = requests.post(
                url,
                headers=headers,
                json = body,
                timeout=self.__timeout__)
        except requests.exceptions.ReadTimeout as e:
            #print(str(e))
            return None, str(e) 
        except requests.exceptions.RequestException as e:
            #print(str(e))
            return None, str(e)
        


        if(response.status_code != requests.codes.ok):            
            return None, str(response.status_code)+response.content.decode("utf-8") 

        return response.json(), None

    def login(self, dss_username, dss_password):
        credentials = {
            "Credentials": {
                "Username": dss_username,
                "Password": dss_password
                }
            }
        headers={}
        headers["Prefer"] = "respond-async"
        resp, err = self.__post__(self.__dss_uri__+self.__auth_endpoint__, headers, credentials)
        if err==None:
            if "value" in resp:
                print("Access Token:", resp["value"])
                self.__token__ =  resp["value"]                
            else:
                raise Exception("login: No key in the response:", resp)                
        else:
            raise Exception("login", err)
            
    def list_all_schedules(self):
         headers={}
         headers["Prefer"] = "respond-async"
         headers["Authorization"] = "Token "+self.__token__
         url = self.__dss_uri__ + self.__schedule_endpoint__
         resp, err = self.__get__(url, headers, None)
         if err==None:
             return resp
         else:
            raise Exception("list_all_schedule", err)

    def get_schedule_by_name(self, schedule_name):
         headers={}
         headers["Prefer"] = "respond-async"
         headers["Authorization"] = "Token "+self.__token__
         url = self.__dss_uri__ + self.__schedule_by_name_endpoint__.format(schedule_name)
         resp, err = self.__get__(url, headers, None)
         if err==None:
             return resp
         else:
            raise Exception("get_schedule_by_name", err)

    def get_last_extraction(self, schedule):
         headers={}
         headers["Prefer"] = "respond-async"
         headers["Authorization"] = "Token "+self.__token__
         url = self.__dss_uri__ + self.__last_extraction_endpoint__.format(schedule["ScheduleId"])
         resp, err = self.__get__(url, headers, None)
         if err==None:
             return resp
         else:
            raise Exception("get_last_extraction", err)

    def get_all_files(self, report_extraction):
         headers={}
         headers["Prefer"] = "respond-async"
         headers["Authorization"] = "Token "+self.__token__
         url = self.__dss_uri__ + self.__all_files_endpoint__.format(report_extraction["ReportExtractionId"])
         resp, err = self.__get__(url, headers, None)
         if err==None:
             return resp
         else:
            raise Exception("get_last_extraction", err)

    def get_file(self, report_extraction, file_type):
         headers={}
         headers["Prefer"] = "respond-async"
         headers["Authorization"] = "Token "+self.__token__
         url = self.__dss_uri__+self.__file_endpoint__.format(report_extraction["ReportExtractionId"])
         if file_type=="note":
             url = url+"NotesFile"
         elif file_type == "ric":
             url = url+"RicMaintenanceFile"
         elif file_type == "data":
             url = url+"FullFile"

         resp, err = self.__get__(url, headers, None)
         if err==None:
             return resp
         else:
            raise Exception("get_file", err)

    def download_file(self, extracted_file, aws):
         decode = True
         headers={}
         headers["Prefer"] = "respond-async"
         headers["Authorization"] = "Token "+self.__token__
         if aws == True:
             headers["X-Direct-Download"] = "True"
         if extracted_file["ExtractedFileName"].endswith("gz") == True:
             decode = False

         requestUrl = self.__dss_uri__+self.__download_file_endpoint__.format(extracted_file["ExtractedFileId"])
         r = requests.get(requestUrl,headers=headers,stream=True)
         chunk_size = 1024 
         with open(extracted_file["ExtractedFileName"], 'wb') as fd:
            for data in r.raw.stream(chunk_size,decode_content=decode):
                fd.write(data)

       