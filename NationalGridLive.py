import requests
import pandas as pd
import xmltodict
from csv import writer
from datetime import datetime
import os


class NationalGridLive(object):
    """
    This class provides a wrapper for the instantaneous data provision of the National
    Grid API, relating to aggregate Natural Gas demand in the UK.

    :param output_directory: the directory to which the outcome is written. (str)
    :param file_name: the file name to which the outcome is written. (str)
    :return: dataset: a Pandas DataFrame containing the output data. (pd.DataFrame)
    """

    def __init__(self,
                 output_directory: str = None,
                 file_name: str = None,
                 verbose: bool = True):

        if verbose:
            print("Evaluating Input Arguments...")

        if file_name:
            if not isinstance(file_name, str):
                raise TypeError("The 'file_name' argument must be a string.")

        if output_directory:
            if not isinstance(output_directory, str):
                raise TypeError("'output_directory' argument must be a string.")

            self.verbose = verbose
            self.file_name = file_name
            self.output_directory = output_directory

    def _append_list_as_row(self, list_of_elements: list):
        """
        This internal function stores each row of data to the directory provided via
        the input arguments of 'file_name' and 'output_directory'. If these arguments
        are not provided, the function is not called.

        :param list_of_elements: the elements to be written to the output file. (List[str])
        :return: N/A
        """
        file_location = os.path.join(self.output_directory, self.file_name)
        if not file_location.endswith(".csv"):
            raise TypeError("The 'file_name' argument should be a .csv file.")
        if not os.path.exists(file_location):
            os.makedirs(file_location)
        if self.verbose:
            print("Writing Output to csv file...")
        with open(file_location, 'a+', newline='') as write_obj:
            csv_writer = writer(write_obj)
            csv_writer.writerow(list_of_elements)

    def collect_data(self):
        """
        This is the main driver function of the code, which collects the data, and returns the outcome to
        memory, and optionally a .csv file. This option is triggered if both the 'file_name' and 'output_directory'
        arguments are satisfied.

        :return:
        """
        if self.verbose:
            print("Retrieving Data...")

        data = []
        url = "http://energywatch.natgrid.co.uk/EDP-PublicUI/PublicPI/InstantaneousFlowWebService.asmx?WSDL"
        headers = {'content-type': 'text/xml'}
        body = """<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <GetInstantaneousFlowData xmlns="http://www.NationalGrid.com/EDP/UI/" />
      </soap:Body>
    </soap:Envelope>"""

        response = requests.post(url, data=body, headers=headers)
        result_xml = response.content
        xml_dict = xmltodict.parse(result_xml)
        total_demand = \
            xml_dict['soap:Envelope']['soap:Body']['GetInstantaneousFlowDataResponse'][
                'GetInstantaneousFlowDataResult'][
                'EDPReportPage']['EDPEnergyGraphTableCollection']['EDPEnergyGraphTableBE'][4]
        total = len(total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'])

        if self.verbose:
            print("Formatting Data...")

        for i in range(0, total):
            entry_name = total_demand['EDPObjectCollection']['EDPObjectBE']['EDPObjectName']
            published_time = total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'][i][
                'ScheduleTime']
            published = datetime.strptime(published_time.replace('T', ' '), '%Y-%m-%d %H:%M:%S')
            publish_str = published.strftime('%d/%m/%Y, %H:%M')
            value = total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'][i][
                'FlowRate']
            time_stamp = total_demand['EDPObjectCollection']['EDPObjectBE']['EnergyDataList']['EDPEnergyDataBE'][i][
                'ApplicableAt']
            applicable_time = datetime.strptime(time_stamp.replace('T', ' '), '%Y-%m-%d %H:%M:%S')
            applicable_time_str = applicable_time.strftime('%d/%m/%Y, %H:%M')
            row_contents = [entry_name, publish_str, value, applicable_time_str, 'N', 'N', '', 'N', 'N']
            data.append(row_contents)
            if self.file_name and self.output_directory:
                self._append_list_as_row(row_contents)

            dataset = pd.DataFrame(data, columns=['NTS Demand Flow', 'Time Published', 'Value', 'Time Applicable',
                                                  'Expired (Y/N)',
                                                  'Amended (Y/N)', 'Amended Timestamp', 'Substituted (Y/N)',
                                                  'Late received (Y/N)'])
            if self.verbose:
                print("Returning Output.")

            return dataset


if __name__ == "main":
    data = NationalGridLive(output_directory=os.getcwd(), file_name="abc.csv", verbose=True)
    print(data)
