import katdal
import numpy as np
import pandas as pd
import pysolr

class SARAOArchiveQuery:
    def __init__(self, start_time, end_time, band):
        self.start_time = start_time
        self.end_time = end_time
        self.band = band
        self.archive = pysolr.Solr('http://kat-archive.kat.ac.za:8983/solr/kat_core')

    def search_archive(self):
        search_str = ['CAS.ProductTypeName:MeerKATTelescopeProduct',
                      f'StartTime:[{self.start_time} TO {self.end_time}]']
        search_string = ' AND '.join(search_str)
        ar_res = self.archive.search(search_string, rows=50, **{'sort': 'StartTime desc'})
        search_results = ar_res.docs
        return search_results

    def process_results(self, search_results):
        imaging_links = []
        imaging_info = []
        for observation in search_results[::-1]:
            if 'CaptureBlockId' in observation and observation['ProposalId'][:3] == 'SCI':
                center_freq = round(observation['CenterFrequency'] + observation['ChannelWidth'])
                bandwidth = observation['Bandwidth']
                num_chan = observation["NumFreqChannels"]
                dump_rate = observation["DumpPeriod"]

                if center_freq == 816000000 and bandwidth == 875e6 and num_chan == num_chan and dump_rate==dump_rate:
                    filename = f"http://archive-gw-1.kat.ac.za:7480/{observation['CaptureBlockId']}/{observation['CaptureBlockId']}_sdp_l0.full.rdb"
                    imaging_links.append(filename)
                    imaging_info.append(katdal.open(filename))
                    print(filename)
        return imaging_links, imaging_info

    def save_to_csv(self, imaging_links, imaging_info):
        imagingdf = pd.DataFrame({'FullLink': imaging_links})
        imagingdf.to_csv(f"sci_Imaging_{self.band}_{self.start_time}_{self.end_time}.csv", index=False)
        imaging_info_df = pd.DataFrame(imaging_info)
        imaging_info_df.to_csv(f"sci_Imaging_full_{self.band}_{self.start_time}_{self.end_time}.csv", index=False)

# Define start, end time strings and band of interest
start_time = '2022-12-01T00:00:00Z'
end_time = '2022-12-31T00:00:00Z'
band = 'U'

# Create SARAOArchiveQuery instance and perform query and processing
archive_query = SARAOArchiveQuery(start_time, end_time, band)
search_results = archive_query.search_archive()
imaging_links, imaging_info = archive_query.process_results(search_results)
archive_query.save_to_csv(imaging_links, imaging_info)


