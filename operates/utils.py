from __future__ import print_function

import logging
import time
from typing import List, Optional, Union
from Authentication import *

import requests
import json

page_size = 100


class Utils(object):

    def __init__(self):
        self.AuthClient = Authentication(api_key)
        self.tgt = self.AuthClient.gettgt()
        self.base_url = "https://uts-ws.nlm.nih.gov"
        self.page_size = 100

    def get_service_ticket(self):
        st = self.AuthClient.getst(self.tgt)
        return st

    @staticmethod
    def parse_page(url: str, start_page: int = 1, **kwargs):
        query_default = {'apiKey': api_key,
                         # 'ticket': self.get_service_ticket(),
                         'pageNumber': start_page,
                         'pageSize': page_size}
        query = dict(query_default, **kwargs)
        r = requests.get(url, params=query)
        # print(r.url)
        r.encoding = 'utf-8'
        return json.loads(r.text)

    @staticmethod
    def map_to_cui(ui: str, source: str, version: str = "current",
                   name: Optional[str] = None, page_number=1) -> (str, Union[str, None], set):
        """
        Use for map ids in other vocabularies to the cui code.
        This method is called recursively in order to move forward to the next page of the result.

        :param ui: original id that need to be mapped to cui
        :param source: original source, e.g.: MSH(Mesh), ICD10, SNOMEDCT_US
        :param version: database version, default "current"
        :param name: the applied name of this ui
        :param page_number: start page of this query, since the results may be divided into multiple pages

        :return: a tuple of CUIs and name:
                    ( original retrieving ui: str,
                      ui's name: str,
                      ui's corresponding cuis: set )
        """
        curr_page_number = page_number
        items = Utils.parse_page("https://uts-ws.nlm.nih.gov" +
                                 "/rest/content/{version}/source/{source}/{ui}/atoms".format(
                                     version=version, source=source, ui=ui),
                                 curr_page_number)
        cuis = set()
        try:

            if not name:
                name_flag = True
            else:
                name_flag = False

            for clause in items["result"]:
                '''
                e.g.: https://uts-ws.nlm.nih.gov/rest/content/2022AB/CUI/C5392097
                '''
                cuis.add(clause["concept"].split('/')[-1])  # get cui
                if name_flag:
                    if clause["termType"] == "PT" and clause["language"] == "ENG":
                        name = clause["name"]  # appoint cui's name, temporarily
                        name_flag = False

            if len(items["result"]) == page_size:
                curr_page_number += 1
                ui, name, next_page_cuis = Utils.map_to_cui(ui, source, version, name, curr_page_number)
                cuis.update(next_page_cuis)
            if name_flag:
                name = items["result"][0]["name"]

            return ui, name, cuis

        except:
            return ui, None, set()

    @staticmethod
    def retrieve_descendants(ui: str, source: str,
                             version: str = "current", page_number=1):
        curr_page_number = page_number
        res = {}  # { ui1: name1, ui2: name2 ...}

        if curr_page_number == 1:  # to retrieve the name of the root ui itself
            root_ui, it_self_name, it_self_cui = Utils.map_to_cui(ui, source)
            res[ui] = it_self_name

        items = Utils.parse_page("https://uts-ws.nlm.nih.gov" +
                                 "/rest/content/{version}/source/{source}/{ui}/descendants"
                                 .format(version=version, source=source, ui=ui), page_number)

        # page_count = items["pageCount"]

        logging.info("Results for page " + str(curr_page_number))
        print("Descendants results for page " + str(curr_page_number))
        # file = open('/home/yutong_guo/Projects/UMLSAPI/results_icd10/{}.tsv'.format(self.identifier), 'a')
        try:
            for result in items["result"]:
                desc_ui = result["ui"]
                name = result["name"]
                res[desc_ui] = name

            logging.info("Page " + str(curr_page_number) + " finished")
            curr_page_number += 1
            # file.close()

            if len(items["result"]) == page_size:  # if multiple pages
                next_page_res = Utils.retrieve_descendants(ui, source, page_number=curr_page_number)
                res.update(next_page_res)

        except KeyError:
            print("{} has no descendants!".format(ui))
            logging.info("No descendants exist")

        return res

    @staticmethod
    def retrieve_cui_sem_type(cui: str, version: str = "current"):
        res = Utils.retrieve_cui_info(cui, version)
        sem_types = res['semanticTypes']
        types_str = []
        for st in sem_types:
            types_str.append(st['name'])

        return types_str

    @staticmethod
    def retrieve_cui_info(cui: str, version: str = "current", page_number=1):
        res = {}
        items = Utils.parse_page("https://uts-ws.nlm.nih.gov" +
                                 "/rest/content/{version}/CUI/{cui}/"
                                 .format(version=version, cui=cui),
                                 page_number)

        try:
            res = items["result"]

        except:
            pass

        return res


    @staticmethod
    def retrieve_cui_name(cui: str, version: str = "current", page_number=1):

        items_new = Utils.parse_page("https://uts-ws.nlm.nih.gov" +
                                     "/rest/content/{version}/CUI/{cui}/atoms".format(
                                         version=version, cui=cui),
                                     page_number)
        names = {cui: set()}
        try:
            inner_page_count = items_new["pageCount"]

            # print("processing page {} in {}".format(inner_page_number, inner_page_count))
            for clause in items_new["result"]:

                if clause["language"] == "ENG" and clause["obsolete"] == "false":
                    # name_by_term_type: set = names[cui].setdefault(clause["termType"], set())
                    # name_by_term_type.add(clause["name"])
                    # names[cui][clause["termType"]] = name_by_term_type
                    names[cui].add(clause["name"])

            if inner_page_count > 1:
                page_number += 1
                next_page_names = Utils.retrieve_cui_name(cui=cui, page_number=page_number)
                # for key in next_page_names[cui].keys():
                #     if key in names[cui].keys():
                #         names[cui][key] = names[cui][key] | next_page_names[cui][key]
                #     else:
                #         names[cui][key] = next_page_names[cui][key]
                names[cui].update(next_page_names[cui])
            return names

        except:
            return {}

    @staticmethod
    def map_sources(ui: str, source: str, target_source: str = "MSH",
                    version: str = "current", page_number=1):
        curr_page_number = page_number
        res = set()

        items = Utils.parse_page("https://uts-ws.nlm.nih.gov" +
                                 "/rest/crosswalk/{version}/source/{source}/{ui}"
                                 .format(version=version, source=source, ui=ui),
                                 page_number, targetSource=target_source)

        # page_count = items["pageCount"]
        print("Map source results for page " + str(curr_page_number))
        try:
            for result in items["result"]:
                target_ui = result["ui"]
                res.add(target_ui)
        except:
            pass

        curr_page_number += 1

        if len(items["result"]) == page_size:  # if multiple pages
            next_page_res = Utils.map_sources(ui, source, page_number=curr_page_number)
            res.update(next_page_res)

        return res

    @staticmethod
    def main():

        # for i in range(2008, 2023):
        #     Utils.retrieve_cui_sem_type(cui="C0006657", version="{}AA".format(str(i)))

        # Utils.retrieve_descendants(ui="D002561", source="MSH")

        # Utils.map_sources(ui="422504002", source="SNOMEDCT_US")

        # Utils.retrieve_cui_info("C1156192")

        Utils.retrieve_cui_name("C1156192")


if __name__ == "__main__":
    Utils.main()
