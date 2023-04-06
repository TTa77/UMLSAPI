import logging
import pickle
import time

import pandas as pd
import os

from tqdm import tqdm


class Predications:
    '''
    This class is for extracting the predications whose pmid are in the pmid set built before.
    The pmid set is decided through CUI corpus, if one pmid contains a CUI that is in the corpus,
    this pmid is included in the pmid set.
    '''
    def __init__(self):
        # set directories
        self.dic_dir = '/home/yutong_guo/Projects/UMLSAPI/temp/set.pkl'
        self.semmed_dir = '/home/yutong_guo/Projects/UMLSAPI/semmed/'

        self.splited_dir = self.semmed_dir + 'splited/'
        self.extracted_dir = self.semmed_dir + 'extracted/'

        # load pmid set
        self.dic = open(self.dic_dir, 'rb')
        self.dic = pickle.load(self.dic)

        # set logging
        logging.basicConfig(filename='/home/yutong_guo/Projects/UMLSAPI/logs/extract_predication.log',
                            level=logging.INFO, format='%(asctime)s - %(message)s')
        logging.info("task initiated at: ".format(time.ctime))

    def get_file_num(self):
        file_ls = os.listdir(self.splited_dir)
        return len(file_ls)

    def load_csv(self, file):
        f = pd.read_csv(filepath_or_buffer=self.splited_dir + file, sep=',',
                        usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
                        names=["PREDICATION_ID", "SENTENCE_ID", "PMID", "PREDICATE",
                               "SUBJECT_CUI", "SUBJECT_NAME", "SUBJECT_SEMTYPE", "SUBJECT_NOVELTY",
                               "OBJECT_CUI", "OBJECT_NAME", "OBJECT_SEMTYPE", "OBJECT_NOVELTY"],
                        encoding="utf-8", encoding_errors="replace")

        return f

    def preserve_lines(self, dataframe: pd.DataFrame):
        dataframe = dataframe[dataframe["PMID"].isin(self.dic)]

        return dataframe

    def write_csv(self, dataframe: pd.DataFrame, file_name: str):
        dataframe.to_csv(path_or_buf=self.extracted_dir + file_name + '.tsv',
                         index=False, header=True, sep="\t")
        return None

    def main(self):
        file_num = self.get_file_num()
        for i in tqdm(range(0, file_num)):
            if i < 10:
                file_name = '000{}'.format(i)
            elif i < 100:
                file_name = '00{}'.format(i)
            elif i < 1000:
                file_name = '0{}'.format(i)
            else:
                file_name = "{}".format(i)

            file_name = "semmed_{}".format(file_name)

            start_time = time.time()
            logging.info("processing {} in {}, start at: {}".
                         format(i+1, file_num, time.asctime(time.localtime(start_time))))
            print("processing {} in {}, start at: {}".
                  format(i+1, file_num, time.asctime(time.localtime(start_time))))

            self.write_csv(dataframe=self.preserve_lines(self.load_csv(file_name)),
                           file_name=file_name)

            end_time = time.time()
            logging.info("{} in {} processed, end at: {}, time cost: {:.2f}".
                         format(i+1, file_num, time.asctime(time.localtime(end_time)), end_time-start_time))
            print("{} in {} processed, end at: {}, time cost: {:.2f}".
                  format(i+1, file_num, time.asctime(time.localtime(end_time)), end_time-start_time))
        return None


if __name__ == "__main__":
    Predications().main()
