!pip install xmltodict
!pip install tqdm

import pandas as pd
from datetime import datetime
import requests
import xmltodict
from csv import writer
from tqdm import tqdm
import os
import time


class NationalGridLive(object):
  '''
  This class provides a wrapper for the instantaneous data provision of the
  National Grid API, relating to total Natural Gas Demand in the UK.

  -------------------------------Arguments------------------------------------------- 

  output_directory: the directory in which the output should be stored.
  file_name: the name of the file within the directory.

  -------------------------------Returns---------------------------------------------

  If no input arguments: dataset: a Pandas DataFrame containing the output data.
  If input arguments: the data is appended to the .csv file specified.  
  '''

  def __init__(self,
               output_directory = None,
               file_name = None,
               verbose = True):
    
    if verbose:
      pbar = tqdm(total=3, position = 0)
      pbar.set_description("Checking if input parameters are correct.")

    if file_name != None:
      if isinstance(file_name, str) is False:
        raise TypeError("'file_name' argument must be a string.")
    
    if output_directory != None:
      if isinstance(output_directory, str) is False:
        raise TypeError("'output_directory' argument must be a string.")


    self.verbose = verbose
    self.pbar = pbar
    self.file_name = file_name
    self.output_directory = output_directory

  def _append_list_as_row(self, file_name, list_of_elements):
    '''
    This internal function stores each row of data to the repositiory specified
    through the input arguments of 'file_name' and 'output_directory'. If no 
    repository is specified, this function is not called.
    '''
    file_location = os.path.join(self.output_directory, self.file_name)

    with open(file_location, 'a+', newline = '') as write_obj:
      csv_writer = writer(write_obj)
      csv_writer.writerow(list_of_elements)
  
  def collect_data(self):
    '''
    This function collects the data, returning the outcome to either a repository,
    as specified by the 'file_name' and 'output_directory' arguments, or to a 
    Pandas DataFrame, if no arguments are provided.
    '''
    if self.verbose:
      self.pbar.update()
      self.pbar.set_description("Retrieving Data.")

    data = []
    url = "http://energywatch.natgrid.co.uk/EDP-PublicUI/PublicPI/InstantaneousFlowWebService.asmx?WSDL"
    headers = {'content-type': 'text/xml'}
    body = """<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetInstantaneousFlowData xmlns="http://www.NationalGrid.com/EDP/UI/" />
      </soap:Body>
    </soap:Envelope>"""

    response = requests.post(url, data=body,headers=headers)
    result_xml = response.content  
    xml_dict = xmltodict.parse(result_xml)
    total_demand = xml_dict['soap:Envelope']['soap:Body']['GetInstantaneousFlowDataResponse']['GetInstantaneousFlowDataResult']['EDPReportPage']['EDPEnergyGraphTableCollection']['EDPEnergyGraphTableBE'][4]
    total = len(total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'])
    
    if self.verbose:
      self.pbar.update()
      self.pbar.set_description("Formatting output data.")

    for i in range(0, total):
      entry_name = total_demand['EDPObjectCollection']['EDPObjectBE']['EDPObjectName']
      published_time = total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'][i]['ScheduleTime']
      published = datetime.strptime(published_time.replace('T', ' '), '%Y-%m-%d %H:%M:%S')
      publish_str = published.strftime('%d/%m/%Y, %H:%M')
      value = total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'][i]['FlowRate']
      time_stamp = total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'][i]['ApplicableAt']
      applicable_time = datetime.strptime(time_stamp.replace('T', ' '), '%Y-%m-%d %H:%M:%S')
      applicable_time_str = applicable_time.strftime('%d/%m/%Y, %H:%M')
      row_contents = [entry_name, publish_str, value, applicable_time_str, 'N', 'N', '', 'N', 'N']
      data.append(row_contents)
      if self.file_name != None and self.output_directory != None:
        self._append_list_as_row(self.file_name, row_contents)

    if self.file_name == None or self.output_directory == None:
      dataset = pd.DataFrame(data, columns=['NTS Demand Flow', 'Time Published', 'Value','Time Applicable','Expired (Y/N)',
                                 'Amended (Y/N)','Amended Timestamp','Substituted (Y/N)','Late received (Y/N)'])
      if self.verbose:
        self.pbar.update()
        self.pbar.set_description("Returning output.")
        time.sleep(5)
        self.pbar.close()

      return dataset
      

if __name__ == "main":
  data = NationalGridLive(verbose = False).collect_data()
