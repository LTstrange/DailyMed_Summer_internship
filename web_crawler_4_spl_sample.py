from urllib.request import urlopen


content = ""

print("linking to the url....")
for line in urlopen('https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/8c3c252c-d9eb-4bbd-b13a-271b87abdffb.xml'):
    line = line.decode('utf-8')
    print('downloading data...')
    content += line

print('already download the data.')
with open('spl_sample.xml', 'w', encoding='utf-8') as file:
    file.write(content)





