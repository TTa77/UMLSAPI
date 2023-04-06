import os
import pandas as pd
import time
import logging

from tqdm import tqdm


class Relations:

    def __init__(self):
        # set global path

        self.sem_corpus_path = "../semmed_v4.3/corpus/"
        self.relations_path = "../semmed_v4.3/relations/"

        # set logging
        # logging.basicConfig(filename='/home/yutong_guo/Projects/UMLSAPI/logs/extract_relations.log',
        #                     level=logging.INFO, format='%(asctime)s - %(message)s')

        self.rel_set = set()
        self.file_path = os.path.join(self.sem_corpus_path, "semmed_filtered.tsv")

    def load_csv(self, file_name):
        df = pd.read_csv(filepath_or_buffer=file_name,
                         sep="\t", header=0, encoding="utf-8", encoding_errors="replace",
                         names=["PREDICATION_ID", "SENTENCE_ID", "PMID", "PREDICATE",
                                "SUBJECT_CUI", "SUBJECT_NAME", "SUBJECT_SEMTYPE",
                                "OBJECT_CUI", "OBJECT_NAME", "OBJECT_SEMTYPE"],
                         )
        df["PREDICATE"] = df["PREDICATE"].str.upper()
        return df

    def write_acord_rel(self, file_name, item, with_header: bool = False):
        item.to_csv(path_or_buf=os.path.join(self.relations_path, file_name + '.tsv'), mode='a',
                    header=with_header, index=False, sep='\t')
        return

    def group(self, df: pd.DataFrame):
        groupls = df.groupby("PREDICATE")
        for pred, details in groupls:
            if pred not in self.rel_set:
                self.rel_set.add(pred)
                self.write_acord_rel(file_name=str(pred), item=details, with_header=True)
            else:
                self.write_acord_rel(file_name=str(pred), item=details, with_header=False)

        return



    @staticmethod
    def main():
        rls = Relations()
        rls.group(rls.load_csv(os.path.join(rls.sem_corpus_path, "semmed_filtered.tsv")))

        return


if __name__ == "__main__":
    Relations.main()
