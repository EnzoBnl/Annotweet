import re
import os
print(os.path.abspath(os.curdir))
import codecs
res = []
with codecs.open("../../../../resources/data/groupe_3.txt", 'r', encoding='utf-8',
                 errors='ignore') as o:
    with codecs.open("../../../../resources/data/groupe_3_annoted.txt", 'r', encoding='utf-8',
                     errors='ignore') as a:
        for tweeto, tweeta in zip(o.read().split("\n"), a.read().split("\n")):
            lab = tweeta[21:24]
            text = tweeto[24:]
            id = tweeto[:21]
            print(">>>>>>><")
            print(lab, text)

            res.append(id + lab + text)
        with codecs.open("groupe_3_annoted_no_fail_aya.txt", 'a+', encoding='utf-8',
                         errors='ignore') as f:
            f.write("\n".join(res))

