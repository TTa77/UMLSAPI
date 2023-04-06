from tqdm import tqdm
from typing.io import IO

import utils
import os


class FindDescendants(utils.Utils):

    def __init__(self):
        super().__init__()
        self.curr_file = None
        self.out_dir = "../results_icd10_mesh/"

    def open_file(self, ui, source_type):
        if source_type == "mesh":
            self.curr_file = open(os.path.join(self.out_dir, "{}_mesh.tsv".format(ui)), "a")
        elif source_type == "icd10":
            self.curr_file = open(os.path.join(self.out_dir, "{}_icd10.tsv".format(ui)), "a")

    def close_file(self):
        self.curr_file.close()

    def write_file(self):
        pass

    def find_descendant(self, ui, source):
        des = self.retrieve_descendants(ui, source)  # { ui1: name1, ui2: name2 ...}
        return des

    def map_cui(self, ui, name, source):
        ui, name, cuis = self.retrieve_cui(ui=ui, name=name, source=source)
        return ui, name, cuis

    def map_mesh(self, ui, source):
        meshs = self.map_sources(ui, source, target_source="MSH")
        return meshs

    @staticmethod
    def main():
        fd = FindDescendants()
        icd10_codes = ["B33.2", "E10", "E11", "E12", "E13", "E14", "E66", "E78",
                       "G45", "G46", "I00-I99.9", "P29", "Q20-Q28.9",
                       "R00", "R01", "R02", "R03", "R73"]

        for code in icd10_codes:
            print("processing {}".format(code))
            print("retrieving descendants...")
            icd10_desc = fd.retrieve_descendants(code, "ICD10")
            cuis = []
            meshs = []
            if len(icd10_desc) > 0:
                print("mapping meshs")
                for k, v in tqdm(sorted(icd10_desc.items())):
                    cuis.append(fd.map_cui(k, v, "ICD10"))
                    mesh = fd.map_sources(k, "ICD10")
                    if len(mesh) > 0:
                        meshs.append((k, v, "|".join(i for i in mesh)))

            print("write out...")
            fd.open_file(code, "icd10")
            fd.curr_file.write("name\tui\tcuis\n")
            for line in cuis:
                fd.curr_file.write(
                    "\t".join(i if type(i) != set else "|".join(j for j in i) for i in line) + "\n"
                )
            fd.close_file()

            if len(meshs) > 0:
                fd.open_file(code, "mesh")
                fd.curr_file.write("name\tui\tmeshs\n")
                for line in meshs:
                    fd.curr_file.write(
                        "\t".join(i for i in line) + "\n"
                    )
                fd.close_file()


if __name__ == "__main__":
    FindDescendants.main()
