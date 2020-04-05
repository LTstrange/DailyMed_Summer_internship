import re
import os

NDC_comp = re.compile(r'(?<=\').{4,14}(?=\')')

files = os.listdir('ndcs')
print(files)

ndc_all = []

for f in files:
    with open('ndcs/' + f, 'r') as ndc_file:
        content = ndc_file.read()
        ndcs = re.findall(NDC_comp, content)
        ndc_all.extend(ndcs)


with open('ndcs.txt', 'w') as file:
    file.write(str(set(ndc_all)))

print(len(ndc_all))




