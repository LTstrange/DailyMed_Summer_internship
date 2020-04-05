import re
import os

setID_comp = re.compile(r'(?<=\')[a-zA-Z0-9\-]+?(?=\')')

files = os.listdir('setIDs')

all_setIDs = set()

for file in files:
    with open('setIDs/' + file, 'r') as file:
        content = file.read()
        setIDs = re.findall(setID_comp, content)
        all_setIDs.update(setIDs)

print(len(all_setIDs))

with open('setIDs.txt', 'w') as file:
    file.write(str(all_setIDs))







